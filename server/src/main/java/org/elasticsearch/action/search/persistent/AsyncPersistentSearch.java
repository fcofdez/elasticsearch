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
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class AsyncPersistentSearch {
    private final Logger logger = LogManager.getLogger(AsyncPersistentSearch.class);

    private final SearchRequest searchRequest;
    private final String asyncSearchId;
    private final SearchTask task;
    private final OriginalIndices originalIndices;
    private final TimeValue expirationTime;
    private final int maxConcurrentQueryRequests;
    private final TransportSearchAction.SearchTimeProvider searchTimeProvider;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;
    private final ThreadPool threadPool;
    private final BiFunction<String, String, Transport.Connection> connectionProvider;
    private final ClusterService clusterService;
    private final ActionListener<Void> onCompletionListener;
    private final AtomicInteger pendingShardsToQueryCount;
    private final ShardQueryResultsReducer shardQueryResultsReducer;
    private final Queue<AsyncShardQueryAndFetch> pendingShardsToQuery;
    private final List<PersistentSearchShard> searchShards;
    private final long startTime;

    private final AtomicInteger runningShardQueries = new AtomicInteger(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Set<Scheduler.ScheduledCancellable> pendingTasks = ConcurrentCollections.newConcurrentSet();

    public AsyncPersistentSearch(SearchRequest searchRequest,
                                 String asyncSearchId,
                                 SearchTask task,
                                 List<PersistentSearchShard> searchShards,
                                 OriginalIndices originalIndices,
                                 TimeValue expirationTime,
                                 int maxConcurrentQueryRequests,
                                 int maxShardsPerReduceRequest,
                                 TransportSearchAction.SearchTimeProvider searchTimeProvider,
                                 SearchShardTargetResolver searchShardTargetResolver,
                                 SearchTransportService searchTransportService,
                                 ThreadPool threadPool,
                                 BiFunction<String, String, Transport.Connection> connectionProvider,
                                 ClusterService clusterService,
                                 ActionListener<Void> onCompletionListener) {
        this.searchRequest = searchRequest;
        this.asyncSearchId = asyncSearchId;
        this.task = task;
        this.originalIndices = originalIndices;
        this.expirationTime = expirationTime;
        this.maxConcurrentQueryRequests = maxConcurrentQueryRequests;
        this.searchTimeProvider = searchTimeProvider;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
        this.threadPool = threadPool;
        this.connectionProvider = connectionProvider;
        this.clusterService = clusterService;
        this.onCompletionListener = onCompletionListener;
        this.startTime = System.nanoTime();

        List<PersistentSearchShard> shardsToSearch = searchShards.stream()
        .filter(searchShard -> searchShard.canBeSkipped() == false)
        .collect(Collectors.toList());

        // We need to search at least 1 shard to generate an empty response
        if (shardsToSearch.isEmpty()) {
            shardsToSearch.add(searchShards.iterator().next());
        }
        //Collections.sort(shardsToSearch);
        // Query and reduce ordering by index name

        this.searchShards = shardsToSearch;
        this.pendingShardsToQueryCount = new AtomicInteger(shardsToSearch.size());
        this.shardQueryResultsReducer = new ShardQueryResultsReducer(this.searchShards.size(), maxShardsPerReduceRequest);
        Queue<AsyncShardQueryAndFetch> pendingShardQueries = new PriorityQueue<>();
        final long expireAbsoluteTime = expirationTime.millis() + searchTimeProvider.getAbsoluteStartMillis();
        for (int i = 0; i < this.searchShards.size(); i++) {
            PersistentSearchShard searchShard = this.searchShards.get(i);
            PersistentSearchShardId persistentSearchShardId = new PersistentSearchShardId(searchShard.getSearchShard(), asyncSearchId, i);
            ShardSearchRequest shardSearchRequest = searchShard.getRequest();
            ExecutePersistentQueryFetchRequest request = new ExecutePersistentQueryFetchRequest(persistentSearchShardId,
                expireAbsoluteTime,
                shardSearchRequest);
            pendingShardQueries.add(new AsyncShardQueryAndFetch(persistentSearchShardId, request));
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
        if (isRunning.compareAndSet(true, false)) {
            pendingTasks.forEach(Scheduler.ScheduledCancellable::cancel);
        }
    }

    boolean isCancelled() {
        return isRunning.get() == false;
    }

    private synchronized void executePendingShardQueries() {
        while (pendingShardsToQuery.peek() != null && runningShardQueries.get() < maxConcurrentQueryRequests) {
            final AsyncShardQueryAndFetch asyncShardQueryAndFetch = pendingShardsToQuery.poll();
            assert asyncShardQueryAndFetch != null;
            runningShardQueries.incrementAndGet();
            asyncShardQueryAndFetch.run();
        }
    }

    private void onShardQuerySuccess(PersistentSearchShardId searchShard, String nodeIdHoldingData) {
        logger.info("Shard query success");
        runningShardQueries.decrementAndGet();
        shardQueryResultsReducer.reduce(new ShardQueryResultInfo(searchShard, nodeIdHoldingData));
        decrementPendingShardsToQuery();
    }

    private void onShardQueryFailure(AsyncShardQueryAndFetch searchShard, Exception e) {
        runningShardQueries.decrementAndGet();

        if (searchShard.canBeRetried() && isCancelled() == false) {
            Scheduler.ScheduledCancellable scheduledTask =
                threadPool.schedule(() -> reEnqueueShardQuery(searchShard), searchShard.nextExecutionDeadline(), ThreadPool.Names.GENERIC);
            pendingTasks.add(scheduledTask);
        } else {
            // Mark shard as failed on the search result? This is an unrecoverable failure
            decrementPendingShardsToQuery();
        }
    }

    private void decrementPendingShardsToQuery() {
        if (pendingShardsToQueryCount.decrementAndGet() > 0) {
            executePendingShardQueries();
        }
    }

    private synchronized void reEnqueueShardQuery(AsyncShardQueryAndFetch searchShard) {
        if (isCancelled()) {
            return;
        }
        pendingShardsToQueryCount.incrementAndGet();
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
        // TODO: Materialize shard failures in the search response
        onCompletionListener.onResponse(null);
    }

    private void sendShardSearchRequest(SearchShardTarget searchShardTarget,
                                        ExecutePersistentQueryFetchRequest asyncShardSearchRequest,
                                        ActionListener<ExecutePersistentQueryFetchResponse> listener) {
        logger.info("Sending shard request!");
        final Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
        searchTransportService.sendExecutePersistentQueryFetchRequest(connection,
            asyncShardSearchRequest,
            task,
            listener
        );
    }

    private Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return connectionProvider.apply(clusterAlias, nodeId);
    }

    private class AsyncShardQueryAndFetch implements Comparable<AsyncShardQueryAndFetch>,
                                                     ActionListener<ExecutePersistentQueryFetchResponse> {
        private final PersistentSearchShardId searchShardId;
        private final ExecutePersistentQueryFetchRequest shardSearchRequest;
        private volatile SearchShardIterator searchShardIterator = null;
        private volatile List<Throwable> failures = null;
        private int retryCount = 0;

        private AsyncShardQueryAndFetch(PersistentSearchShardId searchShardId, ExecutePersistentQueryFetchRequest shardSearchRequest) {
            this.searchShardId = searchShardId;
            this.shardSearchRequest = shardSearchRequest;
        }

        void run() {
            searchShardTargetResolver.resolve(searchShardId.getSearchShard(), originalIndices, new ActionListener<>() {
                @Override
                public void onResponse(SearchShardIterator searchShardIterator) {
                    doRun(searchShardIterator);
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: Handle this
                }
            });
        }

        void doRun(SearchShardIterator searchShardIterator) {
            this.searchShardIterator = searchShardIterator;
            sendRequestToNextShardCopy();
        }

        void sendRequestToNextShardCopy() {
            assert searchShardIterator != null;

            if (isRunning.get() == false) {
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
        public void onResponse(ExecutePersistentQueryFetchResponse response) {
            onShardQuerySuccess(searchShardId, response.getNodeId());
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
            RuntimeException e = new RuntimeException("Unable to execute search on shard " + searchShardId);
            if (failures != null) {
                for (Throwable failure : failures) {
                    e.addSuppressed(failure);
                }
            }
            return e;
        }

        @Override
        public int compareTo(AsyncShardQueryAndFetch o) {
            return searchShardId.compareTo(o.searchShardId);
        }
    }

    private class ShardQueryResultsReducer {
        private final PriorityQueue<ShardQueryResultInfo> pendingShardsToReduce = new PriorityQueue<>();
        private final AtomicReference<ReducePartialPersistentSearchRequest> runningReduce = new AtomicReference<>(null);
        private final AtomicInteger numberOfShardsToReduce;
        private final int maxShardsPerReduceBatch;
        private final AtomicInteger reductionRound = new AtomicInteger(0);

        private ShardQueryResultsReducer(int numberOfShards, int maxShardsPerReduceRequest) {
            this.numberOfShardsToReduce = new AtomicInteger(numberOfShards);
            this.maxShardsPerReduceBatch = Math.min(numberOfShards, maxShardsPerReduceRequest);
        }

        void reduce(ShardQueryResultInfo searchShard) {
            // assert pending shards to reduce
            synchronized (pendingShardsToReduce) {
                pendingShardsToReduce.add(searchShard);
                logger.info("REDUCE {} / {}", pendingShardsToReduce.size(), maxShardsPerReduceBatch);
            }

            maybeRun();
        }

        void maybeRun() {
            if (isRunning.get() == false || runningReduce.get() != null) {
                return;
            }

            final ReducePartialPersistentSearchRequest reducePartialPersistentSearchRequest;
            synchronized (pendingShardsToReduce) {
                if (runningReduce.get() == null && pendingShardsToReduce.size() >= maxShardsPerReduceBatch) {
                    final List<ShardQueryResultInfo> shardsToReduce = new ArrayList<>(maxShardsPerReduceBatch);
                    ShardQueryResultInfo next;
                    while (shardsToReduce.size() <= maxShardsPerReduceBatch && (next = pendingShardsToReduce.poll()) != null) {
                        shardsToReduce.add(next);
                    }

                    final TaskId parentTaskId = task.taskInfo(clusterService.localNode().getId(), false).getTaskId();
                    boolean performFinalReduce = numberOfShardsToReduce.get() == shardsToReduce.size();

                    // Transform the original search request to include the final reduce flag when it applies
                    final SearchRequest updatedRequest = SearchRequest.subSearchRequest(parentTaskId,
                        searchRequest,
                        searchRequest.indices(),
                        null,
                        searchTimeProvider.getAbsoluteStartMillis(),
                        performFinalReduce
                    );

                    final int reductionRound = this.reductionRound.incrementAndGet();

                    reducePartialPersistentSearchRequest = new ReducePartialPersistentSearchRequest(asyncSearchId,
                        shardsToReduce,
                        updatedRequest,
                        searchTimeProvider.getAbsoluteStartMillis(),
                        searchTimeProvider.getRelativeStartNanos(),
                        expirationTime.millis() + searchTimeProvider.getAbsoluteStartMillis(),
                        reductionRound,
                        performFinalReduce
                    );
                    runningReduce.compareAndSet(null, reducePartialPersistentSearchRequest);
                } else {
                    reducePartialPersistentSearchRequest = null;
                }
            }

            if (reducePartialPersistentSearchRequest != null) {
                logger.info("Sending reduce!!");
                sendReduceRequest(reducePartialPersistentSearchRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(ReducePartialPersistentSearchResponse response) {
                        final boolean exchanged = runningReduce.compareAndSet(reducePartialPersistentSearchRequest, null);
                        assert exchanged;
                        final List<PersistentSearchShardFetchFailure> failedFetchShards = response.getFailedToFetchShards();

                        if (failedFetchShards.isEmpty() == false) {
                            for (PersistentSearchShardFetchFailure failedToFetchShard : failedFetchShards) {
                                if (shouldRetryFetchingShardResult(failedToFetchShard.getError())) {
                                    // Retry
                                    reduce(failedToFetchShard.getShard());
                                } else {
                                    // TODO: convert to pending shard and maybe execute the query again against some other node
                                    // be careful with deadlocks if the shard needs to be re-scheduled again
                                }
                            }
                        }

                        final int numberOfReducedShards = response.getReducedShards().size();
                        if (numberOfShardsToReduce.addAndGet(-numberOfReducedShards) == 0) {
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
                        //       Add some versioning to avoid that (right now, we're executing the reduce phase in the coordinator
                        //       node, so this shouldn't happen)
                        logger.info("Error during shards reduce", e);
                        // TODO: Fork
                        //reduceAll(reducePartialPersistentSearchRequest.getShardsToReduce());
                    }
                });
            }
        }

        private boolean shouldRetryFetchingShardResult(Exception e) {
            // TODO: Add a taxonomy of retryable errors
            return true;
        }
    }
}
