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
import java.util.Map;
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
    private final Map<String, AliasFilter> aliasFiltersByIndex;
    private final ActionListener<Void> onCompletionListener;
    private final Queue<AsyncShardQueryAndFetch> pendingShardsToQuery;
    private final int maxConcurrentQueryRequests;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;
    private final SearchTask task;
    private final ThreadPool threadPool;
    private final BiFunction<String, String, Transport.Connection> connectionProvider;
    private final ClusterService clusterService;
    private final AtomicInteger runningShardQueries = new AtomicInteger(0);
    private final AtomicBoolean searchRunning = new AtomicBoolean(true);
    private final ShardQueryResultsReducer shardQueryResultsReducer;
    private final AtomicInteger pendingShardsToQueryCount;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
    private final Logger logger = LogManager.getLogger(AsyncPersistentSearch.class);

    public AsyncPersistentSearch(SearchRequest searchRequest,
                                 String asyncSearchId,
                                 SearchTask task,
                                 List<SearchShard> searchShards,
                                 Map<String, AliasFilter> aliasFiltersByIndex,
                                 OriginalIndices originalIndices,
                                 int maxConcurrentQueryRequests,
                                 SearchShardTargetResolver searchShardTargetResolver,
                                 SearchTransportService searchTransportService,
                                 ThreadPool threadPool,
                                 BiFunction<String, String, Transport.Connection> connectionProvider,
                                 ClusterService clusterService,
                                 TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                 ActionListener<Void> onCompletionListener) {
        this.searchRequest = searchRequest;
        this.asyncSearchId = asyncSearchId;
        this.task = task;
        this.originalIndices = originalIndices;
        this.aliasFiltersByIndex = aliasFiltersByIndex;
        this.onCompletionListener = onCompletionListener;
        this.maxConcurrentQueryRequests = maxConcurrentQueryRequests;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
        this.threadPool = threadPool;
        this.connectionProvider = connectionProvider;
        this.pendingShardsToQueryCount = new AtomicInteger(searchShards.size());
        this.clusterService = clusterService;
        this.shardQueryResultsReducer = new ShardQueryResultsReducer(searchShards.size(), searchShards.size());
        this.searchTimeProvider = searchTimeProvider;
        // Query and reduce ordering by shard
        Queue<AsyncShardQueryAndFetch> pendingShardQueries = new PriorityQueue<>();
        for (SearchShard searchShard : searchShards) {
            final ExecutePersistentQueryFetchRequest request = new ExecutePersistentQueryFetchRequest(asyncSearchId,
                buildShardSearchRequest(originalIndices, searchShard.getShardId()));
            pendingShardQueries.add(new AsyncShardQueryAndFetch(searchShard, request));
        }
        this.pendingShardsToQuery = pendingShardQueries;

    }

    public String getId() {
        return asyncSearchId;
    }

    public void start() {
        executePendingShardQueries();
    }

    public void cancelSearch() {
        searchRunning.compareAndSet(false, true);
    }

    private synchronized void executePendingShardQueries() {
        while (pendingShardsToQuery.peek() != null && runningShardQueries.get() < maxConcurrentQueryRequests) {
            final AsyncShardQueryAndFetch asyncShardQueryAndFetch = pendingShardsToQuery.poll();
            assert asyncShardQueryAndFetch != null;
            runningShardQueries.incrementAndGet();
            asyncShardQueryAndFetch.run();
        }
    }

    private void onShardQuerySuccess(SearchShard searchShard) {
        runningShardQueries.decrementAndGet();
        shardQueryResultsReducer.reduce(searchShard);
        decrementPendingShardsToQuery();
    }

    private void onShardQueryFailure(AsyncShardQueryAndFetch searchShard, Exception e) {
        runningShardQueries.decrementAndGet();

        if (searchShard.canBeRetried()) {
            threadPool.schedule(() -> reEnqueueShardQuery(searchShard), searchShard.nextExecutionDeadline(), ThreadPool.Names.GENERIC);
        } else {
            // Mark shard as failed on the search result? This is an unrecoverable failure
            decrementPendingShardsToQuery();
        }
    }

    private void decrementPendingShardsToQuery() {
        if (pendingShardsToQueryCount.decrementAndGet() == 0) {
            shardQueryResultsReducer.allShardsAreQueried();
        } else {
            executePendingShardQueries();
        }
    }

    private synchronized void reEnqueueShardQuery(AsyncShardQueryAndFetch searchShard) {
        pendingShardsToQuery.add(searchShard);
        executePendingShardQueries();
    }

    private void sendReduceRequest(ReducePartialPersistentSearchRequest request,
                                   ActionListener<ReducePartialPersistentSearchResponse> listener) {
        // For now, execute reduce requests in the coordinator node
        Transport.Connection connection = getConnection(null, clusterService.localNode().getId());
        searchTransportService.sendExecutePartialReduceRequest(connection,
            request,
            task,
            listener
        );
    }

    private void onSearchSuccess() {
        // TODO: Materialize possible shard failures
        onCompletionListener.onResponse(null);
    }

    private void sendShardSearchRequest(SearchShardTarget searchShardTarget,
                                        ExecutePersistentQueryFetchRequest asyncShardSearchRequest,
                                        ActionListener<Void> listener) {
        final Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
        searchTransportService.sendExecutePersistentQueryFetchRequest(connection,
            asyncShardSearchRequest,
            task,
            ActionListener.delegateFailure(listener, (l, r) -> l.onResponse(null))
        );
    }

    private Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return connectionProvider.apply(clusterAlias, nodeId);
    }

    private ShardSearchRequest buildShardSearchRequest(OriginalIndices originalIndices, ShardId shardId) {
        AliasFilter filter = aliasFiltersByIndex.getOrDefault(shardId.getIndexName(), AliasFilter.EMPTY);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            0,
            1, //Hardcoded so the optimization of query + fetch is triggered
            filter,
            1.0f,
            searchTimeProvider.getAbsoluteStartMillis(),
            null,
            null,
            null
        );
        shardRequest.canReturnNullResponseIfMatchNoDocs(false);
        return shardRequest;
    }

    private class AsyncShardQueryAndFetch implements Comparable<AsyncShardQueryAndFetch>, ActionListener<Void> {
        private final SearchShard searchShard;
        private final ExecutePersistentQueryFetchRequest shardSearchRequest;
        private volatile SearchShardIterator searchShardIterator = null;
        private List<Throwable> failures = null;
        private int retryCount = 0;

        private AsyncShardQueryAndFetch(SearchShard searchShard, ExecutePersistentQueryFetchRequest shardSearchRequest) {
            this.searchShard = searchShard;
            this.shardSearchRequest = shardSearchRequest;
        }

        private void run() {
            searchShardIterator = searchShardTargetResolver.resolve(searchShard, originalIndices);
            sendRequestToNextShardCopy();
        }

        private void sendRequestToNextShardCopy() {
            assert searchShardIterator != null;

            if (searchRunning.get() == false) {
                clear();
                return;
            }

            SearchShardTarget target = searchShardIterator.nextOrNull();
            if (target == null) {
                onShardQueryFailure(this, buildException());
                return;
            }

            sendShardSearchRequest(target, shardSearchRequest, this);
        }

        @Override
        public void onResponse(Void unused) {
            onShardQuerySuccess(searchShard);
            clear();
        }

        @Override
        public void onFailure(Exception e) {
            if (failures == null) {
                failures = new ArrayList<>();
            }
            failures.add(e);
            sendRequestToNextShardCopy();
        }

        boolean canBeRetried() {
            if (failures == null) {
                return true;
            }

            for (Throwable failure : failures) {
                if (failure instanceof IndexNotFoundException) {
                    return false;
                }
            }

            return true;
        }

        TimeValue nextExecutionDeadline() {
            return TimeValue.timeValueSeconds(1 << retryCount++);
        }

        void clear() {
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
        public int compareTo(AsyncShardQueryAndFetch o) {
            return searchShard.compareTo(o.searchShard);
        }
    }

    class ShardQueryResultsReducer {
        private final PriorityQueue<SearchShard> pendingShardsToReduce = new PriorityQueue<>();
        private final AtomicReference<ReducePartialPersistentSearchRequest> runningReduce = new AtomicReference<>(null);
        private final AtomicBoolean allShardsAreQueried = new AtomicBoolean(false);
        private final AtomicInteger numberOfShardsToReduce;
        private final int maxShardsPerReduceBatch;

        ShardQueryResultsReducer(int numberOfShards, int maxShardsPerReduceRequest) {
            this.numberOfShardsToReduce = new AtomicInteger(numberOfShards);
            this.maxShardsPerReduceBatch = maxShardsPerReduceRequest;
        }

        void reduce(SearchShard searchShard) {
            assert allShardsAreQueried.get() == false;

            synchronized (pendingShardsToReduce) {
                pendingShardsToReduce.add(searchShard);
            }

            maybeRun();
        }

        void allShardsAreQueried() {
            if (allShardsAreQueried.compareAndSet(false, true)) {
                maybeRun();
            }
        }

        private void reduceAll(List<SearchShard> shards) {
            synchronized (pendingShardsToReduce) {
                pendingShardsToReduce.addAll(shards);
            }
            maybeRun();
        }

        void maybeRun() {
            if (searchRunning.get() == false || runningReduce.get() != null) {
                return;
            }

            final ReducePartialPersistentSearchRequest reducePartialPersistentSearchRequest;
            synchronized (pendingShardsToReduce) {
                if (runningReduce.get() == null && (pendingShardsToReduce.size() >= maxShardsPerReduceBatch || allShardsAreQueried.get())) {
                    final List<SearchShard> shardsToReduce = new ArrayList<>(maxShardsPerReduceBatch);
                    SearchShard next;
                    while (shardsToReduce.size() <= maxShardsPerReduceBatch && (next = pendingShardsToReduce.poll()) != null) {
                        shardsToReduce.add(next);
                    }

                    reducePartialPersistentSearchRequest = new ReducePartialPersistentSearchRequest(asyncSearchId,
                            shardsToReduce,
                            searchRequest,
                            numberOfShardsToReduce.get() == shardsToReduce.size(),
                            searchTimeProvider.getAbsoluteStartMillis(),
                            searchTimeProvider.getRelativeStartNanos()
                        );
                    runningReduce.compareAndSet(null, reducePartialPersistentSearchRequest);
                } else {
                    reducePartialPersistentSearchRequest = null;
                }
            }

            if (reducePartialPersistentSearchRequest != null) {
                sendReduceRequest(reducePartialPersistentSearchRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(ReducePartialPersistentSearchResponse response) {
                        final boolean exchanged = runningReduce.compareAndSet(reducePartialPersistentSearchRequest, null);
                        assert exchanged;
                        final List<SearchShard> reducedShards = response.getReducedShards();
                        if (numberOfShardsToReduce.addAndGet(-reducedShards.size()) == 0) {
                            onSearchSuccess();
                        } else {
                            // TODO: fork as this might cause a stack overflow
                            maybeRun();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final boolean exchanged = runningReduce.compareAndSet(reducePartialPersistentSearchRequest, null);
                        assert exchanged;
                        // TODO: Store the error
                        // TODO: It's possible that we already reduced and stored the intermediate search response
                        //       but due to a network error we don't get the response back and we get an error back.
                        //       Add some versioning to avoid that
                        logger.info("Error! ", e);
                        reduceAll(reducePartialPersistentSearchRequest.getShardsToReduce());
                    }
                });
            }
        }
    }
}
