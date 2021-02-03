/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.stream.Collectors;

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
        private final List<ActionListener<ExecutePersistentQueryFetchResponse>> pendingQueries = new ArrayList<>();
        private final List<Tuple<ActionListener<ReducePartialPersistentSearchResponse>,
                                 ReducePartialPersistentSearchRequest>> pendingReduces = new ArrayList<>();
        private final Executor executor;

        FakeSearchTransportService(Executor executor) {
            super(null, null, null);
            this.executor = executor;
        }

        @Override
        public void sendExecutePersistentQueryFetchRequest(Transport.Connection connection,
                                                           ExecutePersistentQueryFetchRequest request,
                                                           SearchTask task,
                                                           ActionListener<ExecutePersistentQueryFetchResponse> listener) {
            pendingQueries.add(listener);
        }

        void releaseQueryListeners() {
            List<ActionListener<ExecutePersistentQueryFetchResponse>> copy = new ArrayList<>(pendingQueries);
            pendingQueries.clear();
            executor.execute(() -> {
                for (ActionListener<ExecutePersistentQueryFetchResponse> pendingQuery : copy) {
                    pendingQuery.onResponse(new ExecutePersistentQueryFetchResponse());
                }
            });
        }

        void releaseQueryListenersWithError() {
            List<ActionListener<ExecutePersistentQueryFetchResponse>> copy = new ArrayList<>(pendingQueries);
            pendingQueries.clear();
            executor.execute(() -> {
                for (ActionListener<ExecutePersistentQueryFetchResponse> pendingQuery : copy) {
                    pendingQuery.onFailure(new EsRejectedExecutionException());
                }
            });
        }

        @Override
        public void sendExecutePartialReduceRequest(Transport.Connection connection,
                                                    ReducePartialPersistentSearchRequest request,
                                                    SearchTask task,
                                                    ActionListener<ReducePartialPersistentSearchResponse> listener) {
            pendingReduces.add(Tuple.tuple(listener, request));
        }

        void releaseReduceListeners() {
            List<ActionListener<ReducePartialPersistentSearchResponse>> copy =
                pendingReduces.stream().map(Tuple::v1).collect(Collectors.toList());
            pendingReduces.clear();
            executor.execute(() -> {
                for (ActionListener<ReducePartialPersistentSearchResponse> pendingReduce : copy) {
                    pendingReduce.onResponse(new ReducePartialPersistentSearchResponse(List.of()));
                }
            });
        }
    }

    public void testRunSearch() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();


        List<SearchShard> searchShards = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        SearchShardTargetResolver resolver = (shardSearchTarget, originalIndices) -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            return new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE);
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "persistent search", TaskId.EMPTY_TASK_ID,
            Collections.emptyMap());
        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            OriginalIndices.NONE,
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService,
            searchTimeProvider).run();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(5)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListeners();

        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(5)));

        searchTransportService.releaseQueryListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        assertThat(searchTransportService.pendingReduces.get(0).v2().isFinalReduce(), equalTo(false));
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));

        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        assertThat(searchTransportService.pendingReduces.get(0).v2().isFinalReduce(), equalTo(true));

        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
    }

    public void testShardSearchesAreRetriedUntilSearchIsCancelled() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();

        int numOfShards = 2;
        List<SearchShard> searchShards = new ArrayList<>();
        for (int i = 0; i < numOfShards; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        SearchShardTargetResolver resolver = (shardSearchTarget, originalIndices) -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            return new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE);
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "asd", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        final AsyncPersistentSearch asyncPersistentSearch = new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            OriginalIndices.NONE,
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService,
            searchTimeProvider);

        asyncPersistentSearch.run();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(numOfShards)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListenersWithError();
        // Those are retried
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(numOfShards)));

        asyncPersistentSearch.cancelSearch();
        searchTransportService.releaseQueryListenersWithError();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
    }

    public void testShardSearchesAreRetried() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();

        int numOfShards = 2;
        List<SearchShard> searchShards = new ArrayList<>();
        for (int i = 0; i < numOfShards; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        SearchShardTargetResolver resolver = (shardSearchTarget, originalIndices) -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            return new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE);
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            OriginalIndices.NONE,
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService,
            searchTimeProvider).run();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(numOfShards)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListenersWithError();

        // Those are retried
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(numOfShards)));
        searchTransportService.releaseQueryListenersWithError();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(numOfShards)));
        searchTransportService.releaseQueryListeners();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
    }
}
