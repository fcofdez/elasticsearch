/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class AsyncPersistentSearch {
    private final SearchRequest searchRequest;
    private final String asyncSearchId;
    private final OriginalIndices originalIndices;
    private final Queue<AsyncSearchShard> shardSearchTargetQueue;
    private final int maxConcurrentRequests;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;
    private final SearchTask task;
    private final ThreadPool threadPool;
    private final BiFunction<String, String, Transport.Connection> connectionProvider;
    private final ClusterService clusterService;
    private final AtomicInteger runningRequests = new AtomicInteger(0);
    private final AtomicBoolean searchRunning = new AtomicBoolean(true);
    private final ReducePhaseBatcher reducePhaseBatcher;
    private final AtomicInteger shardCount;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
    private final Logger logger = LogManager.getLogger(AsyncPersistentSearch.class);

    public AsyncPersistentSearch(SearchRequest searchRequest,
                                 String asyncSearchId,
                                 SearchTask task,
                                 List<SearchShard> searchShards,
                                 OriginalIndices originalIndices,
                                 int maxConcurrentRequests,
                                 SearchShardTargetResolver searchShardTargetResolver,
                                 SearchTransportService searchTransportService,
                                 ThreadPool threadPool,
                                 BiFunction<String, String, Transport.Connection> connectionProvider,
                                 ClusterService clusterService,
                                 TransportSearchAction.SearchTimeProvider searchTimeProvider) {
        this.searchRequest = searchRequest;
        this.asyncSearchId = asyncSearchId;
        this.originalIndices = originalIndices;
        // Try to query by priority
        Queue<AsyncSearchShard> queue = new PriorityQueue<>();
        for (SearchShard searchShard : searchShards) {
            queue.add(new AsyncSearchShard(searchShard));
        }
        this.shardSearchTargetQueue = queue;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
        this.task = task;
        this.threadPool = threadPool;
        this.connectionProvider = connectionProvider;
        this.shardCount = new AtomicInteger(searchShards.size());
        this.clusterService = clusterService;
        this.reducePhaseBatcher = new ReducePhaseBatcher(5, searchShards.size(), this::onReduceSuccess);
        this.searchTimeProvider = searchTimeProvider;
    }

    public String getId() {
        return asyncSearchId;
    }

    public synchronized void run() {
        if (shardCount.get() <= 0) {
            return;
        }

        while (shardSearchTargetQueue.peek() != null && runningRequests.get() < maxConcurrentRequests) {
            runningRequests.incrementAndGet();
            final AsyncSearchShard target = shardSearchTargetQueue.poll();
            assert target != null;
            target.query(new ActionListener<>() {
                @Override
                public void onResponse(SearchShard searchShard) {
                    onShardQuerySuccess(searchShard);
                }

                @Override
                public void onFailure(Exception e) {
                    onShardQueryFailure(target.searchShard, e);
                }
            });
        }
    }

    public void cancelSearch() {
        // TODO: cleanup stuff
        searchRunning.compareAndSet(false, true);
    }

    void onShardQuerySuccess(SearchShard searchShard) {
        runningRequests.decrementAndGet();
        reducePhaseBatcher.add(searchShard);
        if (shardCount.decrementAndGet() == 0) {
            onAllShardsQueried();
        } else {
            run();
        }
    }

    void onShardQueryFailure(SearchShard searchShard, Exception e) {
        runningRequests.decrementAndGet();
        // Mark shard as failed on the search result? This in theory it's an unrecoverable failure
        if (shardCount.decrementAndGet() == 0) {
            onAllShardsQueried();
        } else {
            run();
        }
    }

    void onAllShardsQueried() {
        reducePhaseBatcher.allShardsAreQueried();
    }

    void onShardsReduced(List<SearchShard> reducedShards) {

    }

    void onReduceFailure(Exception e) {

    }

    void onReduceSuccess() {

    }

    void sendReduceRequest(ReducePartialPersistentSearchRequest request, ActionListener<Void> listener) {
        // For now, execute reduce requests in the coordinator node
        Transport.Connection connection = getConnection(null, clusterService.localNode().getId());
        searchTransportService.sendExecutePartialReduceRequest(connection, request, task, new ActionListener<>() {
            @Override
            public void onResponse(ReducePartialPersistentSearchResponse response) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    void sendShardSearchRequest(final SearchShardTarget searchShardTarget, ActionListener<Void> listener) {
        ShardSearchRequest querySearchRequest =
            buildShardSearchRequest(searchShardTarget.getOriginalIndices(), searchShardTarget.getShardId());
        final ExecutePersistentQueryFetchRequest asyncShardSearchRequest =
            new ExecutePersistentQueryFetchRequest(asyncSearchId, querySearchRequest);

        final Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
        searchTransportService.sendExecutePersistentQueryFetchRequest(connection, asyncShardSearchRequest, task, new ActionListener<>() {
            @Override
            public void onResponse(ExecutePersistentQueryFetchResponse response) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return connectionProvider.apply(clusterAlias, nodeId);
    }

    public final ShardSearchRequest buildShardSearchRequest(OriginalIndices originalIndices, ShardId shardId) {
        // TODO: Handle AliasFilter
        AliasFilter filter = AliasFilter.EMPTY;
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            0,
            1, //Hardcoded so the optimization of query + fetch is triggered
            filter,
            1.0f,
            getAbsoluteStartMillis(),
            null,
            null,
            null
        );
        shardRequest.canReturnNullResponseIfMatchNoDocs(false);
        return shardRequest;
    }

    long getAbsoluteStartMillis() {
        return System.currentTimeMillis();
    }

    // Takes care of run a QUERY + FETCH for a particular shard
    class AsyncSearchShard implements Comparable<AsyncSearchShard> {
        private final SearchShard searchShard;
        private SearchShardIterator searchShardIterator = null;
        private List<Throwable> failures = null;
        private ActionListener<SearchShard> listener = null;
        private int retryCount = 0;

        AsyncSearchShard(SearchShard searchShard) {
            this.searchShard = searchShard;
        }

        void query(ActionListener<SearchShard> listener) {
            this.listener = listener;
            execute();
        }

        void execute() {
            searchShardIterator = searchShardTargetResolver.resolve(searchShard, originalIndices);
            doExecute();
        }

        void doExecute() {
            if (searchRunning.get() == false) {
                clear();
                return;
            }

            SearchShardTarget target;
            if (searchShardIterator == null || (target = searchShardIterator.nextOrNull()) == null) {
                tryToRunAgain();
                return;
            }

            sendShardSearchRequest(target, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    listener.onResponse(searchShard);
                    clear();
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: mark shard failed directly on IndexNotFoundException
                    onShardFailure(e);
                }
            });
        }

        void tryToRunAgain() {
            if (canBeRetried() == false) {
                listener.onFailure(buildException());
                clear();
                return;
            }

            threadPool.schedule(this::execute, TimeValue.timeValueSeconds(1 << retryCount++), ThreadPool.Names.GENERIC);
        }

        boolean canBeRetried() {
            if (searchRunning.get() == false) {
                return false;
            }

            // It means that the shard wasn't allocated at that point
            if (failures == null) {
                return true;
            }

            for (Throwable failure : failures) {
                // Bypass this on IndexNotFound
                if (failure instanceof IndexNotFoundException) {
                    return false;
                }
            }

            return true;
        }

        void onShardFailure(Exception e) {
            if (failures == null) {
                failures = new ArrayList<>();
            }
            failures.add(e);
            doExecute();
        }

        void clear() {
            listener = null;
            searchShardIterator = null;
            if (failures != null) {
                failures.clear();
            }
        }

        RuntimeException buildException() {
            RuntimeException e = new RuntimeException("Unable to execute search on shard " + searchShard);
            if (failures != null) {
                for (Throwable failure : failures) {
                    e.addSuppressed(failure);
                }
            }
            return e;
        }

        @Override
        public int compareTo(AsyncSearchShard o) {
            return searchShard.compareTo(o.searchShard);
        }
    }

    class ReducePhaseBatcher {
        private final int SHARDS_PER_REDUCE = 5;
        private final PriorityQueue<SearchShard> pendingShardsToReduce = new PriorityQueue<>();
        private final AtomicReference<ReducePartialPersistentSearchRequest> runningReduce = new AtomicReference<>(null);
        private boolean completed = false;
        private final AtomicInteger numberOfShardsToReduce;
        private final AtomicInteger shardToReduceCount;
        private final Runnable onFinish;

        ReducePhaseBatcher(int numberOfShardsToReduce, int numberOfShards, Runnable onFinish) {
            this.numberOfShardsToReduce = new AtomicInteger(numberOfShardsToReduce);
            this.shardToReduceCount = new AtomicInteger(numberOfShards);
            this.onFinish = onFinish;
        }

        synchronized void add(SearchShard searchShard) {
            assert completed == false;

            pendingShardsToReduce.add(searchShard);
            if (pendingShardsToReduce.size() == SHARDS_PER_REDUCE) {
                executeNext();
            }
        }

        synchronized void allShardsAreQueried() {
            completed = true;
            executeNext();
        }

        synchronized void executeNext() {
            if (searchRunning.get() == false || runningReduce.get() != null) {
                return;
            }

            int shardsToReduce = 0;
            final List<SearchShard> shards = new ArrayList<>(SHARDS_PER_REDUCE);
            SearchShard next;
            while ((next = pendingShardsToReduce.poll()) != null && shardsToReduce++ < SHARDS_PER_REDUCE) {
                shards.add(next);
            }

            if (shards.isEmpty()) {
                return;
            }

            final ReducePartialPersistentSearchRequest reducePartialResultsRequest =
                new ReducePartialPersistentSearchRequest(asyncSearchId,
                    shards,
                    searchRequest,
                    shardToReduceCount.get() == shards.size(),
                    searchTimeProvider.getAbsoluteStartMillis(),
                    searchTimeProvider.getRelativeStartNanos()
                );
            final boolean exchanged = runningReduce.compareAndSet(null, reducePartialResultsRequest);
            assert exchanged;

            sendReduceRequest(reducePartialResultsRequest, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    final boolean exchanged = runningReduce.compareAndSet(reducePartialResultsRequest, null);
                    assert exchanged;
                    shardToReduceCount.addAndGet(-shards.size());
                    onShardsReduced(shards);
                    // Keep track of reduced shards
                    // TODO: This is executed in a IO thread, we shouldn't block there?
                    if (numberOfShardsToReduce.decrementAndGet() > 0) {
                        executeNext();
                    } else {
                        onAllShardsReduced();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    runningReduce.compareAndSet(reducePartialResultsRequest, null);
                    onReduceFailure(e);
                    // TODO: Inspect error and maybe retry somewhere else?
                    // TODO: Store the error somewhere
                    pendingShardsToReduce.addAll(shards);
                    executeNext();
                }
            });
        }

        void onAllShardsReduced() {
            try {
                onFinish.run();
            } catch (Exception e) {
                // Unable to notify completion
            }
        }
    }
}
