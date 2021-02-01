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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.persistent.AsyncPersistentSearch;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AsyncShardSearchRequest;
import org.elasticsearch.search.internal.ReducePartialResultsRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.IsEqual.equalTo;

public class AsyncPersistentSearchTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = new ClusterService(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    static class FakeSearchTransportService extends SearchTransportService {
        private final Logger logger = LogManager.getLogger(FakeSearchTransportService.class);
        final List<ActionListener<TransportResponse.Empty>> pendingQueries = new ArrayList<>();
        final List<ActionListener<TransportResponse.Empty>> pendingReduces = new ArrayList<>();
        private final Executor executor;

        public FakeSearchTransportService(Executor executor) {
            super(null, null, null);
            this.executor = executor;
        }

        @Override
        public synchronized void sendExecutePersistentQueryFetch(Transport.Connection connection,
                                                                 AsyncShardSearchRequest request,
                                                                 SearchTask task,
                                                                 ActionListener<TransportResponse.Empty> listener) {
            pendingQueries.add(listener);
        }

        void releaseQueryListeners() {
            List<ActionListener<TransportResponse.Empty>> copy = new ArrayList<>(pendingQueries);
            pendingQueries.clear();
            executor.execute(() -> {
                for (ActionListener<TransportResponse.Empty> pendingQuery : copy) {
                    pendingQuery.onResponse(TransportResponse.Empty.INSTANCE);
                }
            });
        }

        void releaseQueryListenersWithError() {
            List<ActionListener<TransportResponse.Empty>> copy = new ArrayList<>(pendingQueries);
            pendingQueries.clear();
            executor.execute(() -> {
                for (ActionListener<TransportResponse.Empty> pendingQuery : copy) {
                    pendingQuery.onFailure(new EsRejectedExecutionException());
                }
            });
        }


        void releaseReduceListeners() {
            List<ActionListener<TransportResponse.Empty>> copy = new ArrayList<>(pendingReduces);
            pendingReduces.clear();
            executor.execute(() -> {
                for (ActionListener<TransportResponse.Empty> pendingReduce : copy) {
                    pendingReduce.onResponse(TransportResponse.Empty.INSTANCE);
                }
            });
        }



        @Override
        public void sendExecutePartialReduce(Transport.Connection connection,
                                             ReducePartialResultsRequest request,
                                             SearchTask task,
                                             ActionListener<TransportResponse.Empty> listener) {
            pendingReduces.add(listener);
            //listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public void testRunSearch() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();


        List<SearchShard> searchShards = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        AsyncPersistentSearch.ShardSearchTargetResolver resolver = shardSearchTarget -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            return new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE);
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "asd", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService).run();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(5)));

        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(5)));
        searchTransportService.releaseQueryListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
    }

    public void testShardSearchesAreRetried() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();


        List<SearchShard> searchShards = new ArrayList<>(10);
        for (int i = 0; i < 2; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        AsyncPersistentSearch.ShardSearchTargetResolver resolver = shardSearchTarget -> {
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
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService);

        asyncPersistentSearch.run();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(2)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListenersWithError();
        // Those are retried
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(2)));
        asyncPersistentSearch.cancelSearch();
        searchTransportService.releaseQueryListenersWithError();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
    }

    public void testShardSearchesAreRetriedUntilSearchIsCancelled() throws Exception {
        final SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final String persistentSearchId = UUIDs.randomBase64UUID();


        List<SearchShard> searchShards = new ArrayList<>(10);
        for (int i = 0; i < 2; i++) {
            searchShards.add(new SearchShard(null, new ShardId("index", "_na_", i)));
        }

        AsyncPersistentSearch.ShardSearchTargetResolver resolver = shardSearchTarget -> {
            final ShardId shardId = shardSearchTarget.getShardId();
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = ShardRoutingHelper.initialize(shardRouting, "nodeId");
            return new SearchShardIterator(null, shardId, List.of(shardRouting), OriginalIndices.NONE);
        };
        final SearchTask searchTask = new SearchTask(0, "search", "action", () -> "asd", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        FakeSearchTransportService searchTransportService = new FakeSearchTransportService(threadPool.executor(ThreadPool.Names.GENERIC));
        new AsyncPersistentSearch(searchRequest,
            persistentSearchId,
            searchTask,
            searchShards,
            5,
            resolver,
            searchTransportService,
            threadPool,
            (cluster, node) -> null,
            clusterService).run();

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(2)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
        searchTransportService.releaseQueryListenersWithError();
        // Those are retried
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(2)));
        searchTransportService.releaseQueryListenersWithError();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(2)));

        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(1)));
        searchTransportService.releaseReduceListeners();
        assertBusy(() -> assertThat(searchTransportService.pendingQueries.size(), equalTo(0)));
        assertBusy(() -> assertThat(searchTransportService.pendingReduces.size(), equalTo(0)));
    }


}
