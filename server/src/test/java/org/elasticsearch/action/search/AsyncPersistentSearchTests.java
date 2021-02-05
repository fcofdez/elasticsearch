/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.persistent.AsyncPersistentSearch;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchRequest;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchResponse;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchResponse;
import org.elasticsearch.action.search.persistent.SearchShardTargetResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.IsEqual.equalTo;

public class AsyncPersistentSearchTests extends ESTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportSearchAction.SearchTimeProvider searchTimeProvider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        searchTimeProvider = new TransportSearchAction.SearchTimeProvider(System.currentTimeMillis(),
            threadPool.relativeTimeInNanos(), threadPool::relativeTimeInNanos);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    static class FakeSearchTransportService extends SearchTransportService {
        private final List<ActionListener<ExecutePersistentQueryFetchResponse>> inFlightQueries = new ArrayList<>();
        private final List<Tuple<ActionListener<ReducePartialPersistentSearchResponse>,
            ReducePartialPersistentSearchRequest>> inFlightReduces = new ArrayList<>();
        private final Executor executor;
        private final AtomicInteger docIdGenerator = new AtomicInteger();

        FakeSearchTransportService(Executor executor) {
            super(null, null, null);
            this.executor = executor;
        }

        @Override
        public synchronized void sendExecutePersistentQueryFetchRequest(Transport.Connection connection,
                                                                        ExecutePersistentQueryFetchRequest request,
                                                                        SearchTask task,
                                                                        ActionListener<ExecutePersistentQueryFetchResponse> listener) {
            inFlightQueries.add(listener);
        }

        void respondToInFlightQueriesSuccessfully() {
            var inFlightQueriesCopy = new ArrayList<>(inFlightQueries);
            inFlightQueries.clear();
            executor.execute(() -> {
                for (ActionListener<ExecutePersistentQueryFetchResponse> pendingQuery : inFlightQueriesCopy) {
                    pendingQuery.onResponse(new ExecutePersistentQueryFetchResponse("search" + docIdGenerator.incrementAndGet()));
                }
            });
        }

        void respondToInFlightQueriesWithAnError() {
            var inFlightQueriesCopy = new ArrayList<>(inFlightQueries);
            inFlightQueries.clear();
            executor.execute(() -> {
                for (ActionListener<ExecutePersistentQueryFetchResponse> pendingQuery : inFlightQueriesCopy) {
                    pendingQuery.onFailure(new EsRejectedExecutionException());
                }
            });
        }

        int inFlightQueriesCount() {
            return inFlightQueries.size();
        }

        @Override
        public synchronized void sendExecutePartialReduceRequest(Transport.Connection connection,
                                                                 ReducePartialPersistentSearchRequest request,
                                                                 SearchTask task,
                                                                 ActionListener<ReducePartialPersistentSearchResponse> listener) {
            inFlightReduces.add(Tuple.tuple(listener, request));
        }

        synchronized void respondToInFlightReducesSuccessfully() {
            var inFlightReducesCopy = new ArrayList<>(inFlightReduces);
            inFlightReduces.clear();
            executor.execute(() -> {
                for (Tuple<ActionListener<ReducePartialPersistentSearchResponse>, ReducePartialPersistentSearchRequest> pendingReduce :
                    inFlightReducesCopy) {
                    pendingReduce.v1().onResponse(new ReducePartialPersistentSearchResponse(pendingReduce.v2().getShardsToReduce()));
                }
            });
        }

        synchronized int inFlightReducesCount() {
            return inFlightReduces.size();
        }

        synchronized ReducePartialPersistentSearchRequest getReduceRequest(int index) {
            return inFlightReduces.get(index).v2();
        }
    }

    public void testRunSearch() throws Exception {
        List<SearchShard> searchShards = createSearchShards(10);

        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        getAsyncPersistentSearch(5, 5, searchShards, searchTransportService).start();

        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(5)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
        searchTransportService.respondToInFlightQueriesSuccessfully();

        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(1)));
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(5)));

        searchTransportService.respondToInFlightQueriesSuccessfully();
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(1)));
        assertThat(searchTransportService.getReduceRequest(0).performFinalReduce(), equalTo(false));
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(0)));

        searchTransportService.respondToInFlightReducesSuccessfully();
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(1)));
        assertThat(searchTransportService.getReduceRequest(0).performFinalReduce(), equalTo(true));

        searchTransportService.respondToInFlightReducesSuccessfully();
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
    }

    public void testShardSearchesAreRetriedUntilSearchIsCancelled() throws Exception {
        int numOfShards = 2;
        List<SearchShard> searchShards = createSearchShards(numOfShards);

        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        AsyncPersistentSearch asyncPersistentSearch =
            getAsyncPersistentSearch(5, searchShards.size(), searchShards, searchTransportService);
        asyncPersistentSearch.start();

        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(numOfShards)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
        searchTransportService.respondToInFlightQueriesWithAnError();
        // Failed Queries are retried
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(numOfShards)));

        asyncPersistentSearch.cancelSearch();
        if (randomBoolean()) {
            searchTransportService.respondToInFlightQueriesWithAnError();
        } else {
            searchTransportService.respondToInFlightQueriesSuccessfully();
        }
        // After the search has been cancelled, there aren't new reduce or query requests
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
    }

    public void testShardSearchesAreRetried() throws Exception {
        int numOfShards = 2;
        List<SearchShard> searchShards = createSearchShards(numOfShards);

        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        getAsyncPersistentSearch(5, numOfShards, searchShards, searchTransportService).start();

        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(numOfShards)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
        searchTransportService.respondToInFlightQueriesWithAnError();

        // Failed queries are retried until they succeed
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(numOfShards)));
        searchTransportService.respondToInFlightQueriesWithAnError();
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(numOfShards)));
        searchTransportService.respondToInFlightQueriesSuccessfully();

        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(1)));
        searchTransportService.respondToInFlightReducesSuccessfully();
        assertBusy(() -> assertThat(searchTransportService.inFlightQueriesCount(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.inFlightReducesCount(), equalTo(0)));
    }

    private AsyncPersistentSearch getAsyncPersistentSearch(int maxConcurrentQueryRequests,
                                                           int maxShardsPerReduceRequest,
                                                           List<SearchShard> searchShards,
                                                           FakeSearchTransportService searchTransportService) {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();
        SearchShardTargetResolver resolver = (shardSearchTarget, originalIndices, listener) -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            listener.onResponse(new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE));
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "persistent search", TaskId.EMPTY_TASK_ID,
            Collections.emptyMap());

        return new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            OriginalIndices.NONE,
            Collections.emptyMap(),
            TimeValue.timeValueHours(1),
            maxConcurrentQueryRequests,
            maxShardsPerReduceRequest,
            searchTimeProvider,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService,
            ActionListener.wrap(() -> {
            }));
    }

    private List<SearchShard> createSearchShards(int numShards) {
        List<SearchShard> searchShards = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }
        return searchShards;
    }
}
