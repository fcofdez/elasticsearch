/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.FieldDataProvider;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.elasticsearch.action.search.SearchAsyncActionTests.getShardsIter;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CanMatchPreFilterSearchPhaseTests extends ESTestCase {

    public void testFilterShards() throws InterruptedException {

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();

        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(request.shardId().id() == 0 ? shard1 :
                    shard2, null))).start();
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
            searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
            (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        result.set(iter);
                        latch.countDown();
                    }}, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();

        if (shard1 && shard2) {
            for (SearchShardIterator i : result.get()) {
                assertFalse(i.skip());
            }
        } else if (shard1 == false &&  shard2 == false) {
            assertFalse(result.get().get(0).skip());
            assertTrue(result.get().get(1).skip());
        } else {
            assertEquals(0, result.get().get(0).shardId().id());
            assertEquals(1, result.get().get(1).shardId().id());
            assertEquals(shard1, !result.get().get(0).skip());
            assertEquals(shard2, !result.get().get(1).skip());
        }
    }

    public void testFilterWithFailure() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                boolean throwException = request.shardId().id() != 0;
                if (throwException && randomBoolean()) {
                    throw new IllegalArgumentException("boom");
                } else {
                    new Thread(() -> {
                        if (throwException == false) {
                            listener.onResponse(new SearchService.CanMatchResponse(shard1, null));
                        } else {
                            listener.onFailure(new NullPointerException());
                        }
                    }).start();
                }
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
            searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
            (iter) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    result.set(iter);
                    latch.countDown();
                }}, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();

        assertEquals(0, result.get().get(0).shardId().id());
        assertEquals(1, result.get().get(1).shardId().id());
        assertEquals(shard1, !result.get().get(0).skip());
        assertFalse(result.get().get(1).skip()); // never skip the failure
    }

    /*
     * In cases that a query coordinating node held all the shards for a query, the can match phase would recurse and end in stack overflow
     * when subjected to max concurrent search requests. This test is a test for that situation.
     */
    public void testLotsOfShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        final Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        final DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));


        final SearchTransportService searchTransportService =
            new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(
                    Transport.Connection connection,
                    ShardSearchRequest request,
                    SearchTask task,
                    ActionListener<SearchService.CanMatchResponse> listener) {
                    listener.onResponse(new SearchService.CanMatchResponse(randomBoolean(), null));
                }
            };

        final CountDownLatch latch = new CountDownLatch(1);
        final OriginalIndices originalIndices = new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS);
        final GroupShardsIterator<SearchShardIterator> shardsIter =
            getShardsIter("idx", originalIndices, 4096, randomBoolean(), primaryNode, replicaNode);
        final ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        SearchTransportService transportService = new SearchTransportService(null, null, null);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(response -> {},
            (e) -> { throw new AssertionError("unexpected", e);});
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        final CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            EsExecutors.newDirectExecutorService(),
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            (iter) -> new AbstractSearchAsyncAction<>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                        assert cluster == null : "cluster was not null: " + cluster;
                        return lookup.get(node);
                    },
                aliasFilters,
                Collections.emptyMap(),
                Collections.emptyMap(),
                executor,
                searchRequest,
                responseListener,
                iter,
                new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
                ClusterState.EMPTY_STATE,
                null,
                new ArraySearchPhaseResults<>(iter.size()),
                randomIntBetween(1, 32),
                SearchResponse.Clusters.EMPTY) {

                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                    return new SearchPhase("test") {
                        @Override
                        public void run() {
                            latch.countDown();
                        }
                    };
                }

                @Override
                protected void executePhaseOnShard(
                    final SearchShardIterator shardIt,
                    final SearchShardTarget shard,
                    final SearchActionListener<SearchPhaseResult> listener) {
                    if (randomBoolean()) {
                        listener.onResponse(new SearchPhaseResult() {});
                    } else {
                        listener.onFailure(new Exception("failure"));
                    }
                }
            }, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();
        executor.shutdown();
    }

    public void testSortShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            List<ShardId> shardIds = new ArrayList<>();
            List<MinAndMax<?>> minAndMaxes = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                         ActionListener<SearchService.CanMatchResponse> listener) {
                    Long min = rarely() ? null : randomLong();
                    Long max = min == null ? null  : randomLongBetween(min, Long.MAX_VALUE);
                    MinAndMax<?> minMax = min == null ? null : new MinAndMax<>(min, max);
                    boolean canMatch = frequently();
                    synchronized (shardIds) {
                        shardIds.add(request.shardId());
                        minAndMaxes.add(minMax);
                        if (canMatch == false) {
                            shardToSkip.add(request.shardId());
                        }
                    }
                    new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(canMatch, minMax))).start();
                }
            };

            AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("logs",
                new OriginalIndices(new String[]{"logs"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
                randomIntBetween(2, 20), randomBoolean(), primaryNode, replicaNode);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
                searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
                (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        result.set(iter);
                        latch.countDown();
                    }
                }, SearchResponse.Clusters.EMPTY);

            canMatchPhase.start();
            latch.await();
            ShardId[] expected = IntStream.range(0, shardIds.size())
                .boxed()
                .sorted(Comparator.comparing(minAndMaxes::get, MinAndMax.getComparator(order)).thenComparing(shardIds::get))
                .map(shardIds::get)
                .toArray(ShardId[]::new);

            int pos = 0;
            for (SearchShardIterator i : result.get()) {
                assertEquals(shardToSkip.contains(i.shardId()), i.skip());
                assertEquals(expected[pos++], i.shardId());
            }
        }
    }

    public void testInvalidSortShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        for (SortOrder order : SortOrder.values()) {
            int numShards = randomIntBetween(2, 20);
            List<ShardId> shardIds = new ArrayList<>();
            Set<ShardId> shardToSkip = new HashSet<>();

            SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
                @Override
                public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                         ActionListener<SearchService.CanMatchResponse> listener) {
                    final MinAndMax<?> minMax;
                    if (request.shardId().id() == numShards-1) {
                        minMax = new MinAndMax<>(new BytesRef("bar"), new BytesRef("baz"));
                    } else {
                        Long min = randomLong();
                        Long max = randomLongBetween(min, Long.MAX_VALUE);
                        minMax = new MinAndMax<>(min, max);
                    }
                    boolean canMatch = frequently();
                    synchronized (shardIds) {
                        shardIds.add(request.shardId());
                        if (canMatch == false) {
                            shardToSkip.add(request.shardId());
                        }
                    }
                    new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(canMatch, minMax))).start();
                }
            };

            AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("logs",
                new OriginalIndices(new String[]{"logs"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
                numShards, randomBoolean(), primaryNode, replicaNode);
            final SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp").order(order)));
            searchRequest.allowPartialSearchResults(true);

            CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
                searchRequest, null, shardsIter, timeProvider, ClusterState.EMPTY_STATE, null,
                (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        result.set(iter);
                        latch.countDown();
                    }
                }, SearchResponse.Clusters.EMPTY);

            canMatchPhase.start();
            latch.await();
            int shardId = 0;
            for (SearchShardIterator i : result.get()) {
                assertThat(i.shardId().id(), equalTo(shardId++));
                assertEquals(shardToSkip.contains(i.shardId()), i.skip());
            }
            assertThat(result.get().size(), equalTo(numShards));
        }
    }


    public void testApplyCanMatchOnCoordinatorWithQueriesThatCannotBeResolvedInTheCoordinator() throws Exception {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        OriginalIndices originalIndices = new OriginalIndices(new String[]{dataStream.getName()}, SearchRequest.DEFAULT_INDICES_OPTIONS);

        GroupShardsIterator<SearchShardIterator> shardsIter = groupShardsIterator(
            getShardsIter(dataStreamIndex1, originalIndices, 2, false, primaryNode, null),
            getShardsIter(dataStreamIndex2, originalIndices, 2, false, primaryNode, null)
        );

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(dataStream.getName());
        searchRequest.allowPartialSearchResults(true);

        long indexMinTimestamp = 10;
        long indexMaxTimestamp = 20;
        FakeCoordinatorRewriteContextProvider contextProvider = new FakeCoordinatorRewriteContextProvider();
        String timestampFieldName = dataStream.getTimeStampField().getName();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProvider.addIndex(dataStreamIndex, timestampFieldName, indexMinTimestamp, indexMaxTimestamp);
        }

        // We query a range outside of the timestamp range covered by both datastream indices
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName)
            .from(indexMaxTimestamp + 1)
            .to(indexMaxTimestamp + 2);

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("fake", "value");

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .should(rangeQueryBuilder)
            .should(termQueryBuilder);

        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        if (randomBoolean()) {
            // Apply the query on the request body
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
            searchSourceBuilder.query(queryBuilder);
            searchRequest.source(searchSourceBuilder);

            AliasFilter emptyAliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
            for (Index dataStreamIndex : dataStream.getIndices()) {
                aliasFilters.put(dataStreamIndex.getUUID(), emptyAliasFilter);
            }
        } else {
            // Apply the query as an alias filter
            AliasFilter aliasFilter = new AliasFilter(queryBuilder, Strings.EMPTY_ARRAY);
            for (Index dataStreamIndex : dataStream.getIndices()) {
                aliasFilters.put(dataStreamIndex.getUUID(), aliasFilter);
            }
        }

        executeCanMatchPhase(lookup, shardsIter, searchRequest, contextProvider, aliasFilters,
            (searchShardIterators, requests) -> {
                int skippedShards = 0;
                int nonSkippedShards = 0;
                for (List<SearchShardIterator> shardIterators : searchShardIterators.values()) {
                    for (SearchShardIterator shardIterator : shardIterators) {
                        if (shardIterator.skip()) {
                            skippedShards++;
                        } else {
                            nonSkippedShards++;
                        }
                    }
                }

                // The query included a term query in a should clause so we should
                // send the request in order to be sure if we can skip that shard or not
                assertThat(skippedShards, equalTo(0));
                assertThat(nonSkippedShards, equalTo(shardsIter.size()));
                assertThat(requests.size(), equalTo(shardsIter.size()));
            }
        );
    }

    public void testApplyCanMatchOnCoordinator() throws Exception {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        OriginalIndices originalIndices = new OriginalIndices(new String[]{dataStream.getName()}, SearchRequest.DEFAULT_INDICES_OPTIONS);

        int assignedPrimaries = 0;
        List<SearchShardIterator> shardIters = new ArrayList<>();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            boolean withAssignedPrimaries = randomBoolean();
            int numShards = randomIntBetween(1, 6);
            shardIters.addAll(
                getShardsIter(dataStreamIndex,
                    originalIndices,
                    numShards,
                    false,
                    // If we have to execute the can match request against all the shards
                    // and none is assigned, the phase is considered as failed meaning that the next phase won't be executed
                    withAssignedPrimaries || assignedPrimaries == 0 ? primaryNode : null,
                    null)
            );
            assignedPrimaries += withAssignedPrimaries ? numShards : 0;
        }
        GroupShardsIterator<SearchShardIterator> shardsIter = GroupShardsIterator.sortAndCreate(shardIters);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(dataStream.getName());
        searchRequest.allowPartialSearchResults(true);

        long indexMinTimestamp = 10;
        long indexMaxTimestamp = 20;
        FakeCoordinatorRewriteContextProvider contextProvider = new FakeCoordinatorRewriteContextProvider();
        String timestampFieldName = dataStream.getTimeStampField().getName();
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProvider.addIndex(dataStreamIndex, timestampFieldName, indexMinTimestamp, indexMaxTimestamp);
        }

        boolean canFilterDataStreamIndices = randomBoolean();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(timestampFieldName);
        // We query a range outside of the timestamp range covered by both datastream indices
        if (canFilterDataStreamIndices) {
            rangeQueryBuilder
                .from(indexMaxTimestamp + 1)
                .to(indexMaxTimestamp + 2);
        } else {
            rangeQueryBuilder
                .from(indexMinTimestamp)
                .to(indexMaxTimestamp);
        }

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .filter(rangeQueryBuilder);

        if (randomBoolean()) {
            // Add an additional filter that cannot be evaluated in the coordinator
            queryBuilder.filter(new TermQueryBuilder("fake", "value"));
        }

        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        // TODO alias filter && query
        // Apply the query on the request body
        if (randomBoolean()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
            searchSourceBuilder.query(queryBuilder);
            searchRequest.source(searchSourceBuilder);

            AliasFilter emptyAliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
            for (Index dataStreamIndex : dataStream.getIndices()) {
                aliasFilters.put(dataStreamIndex.getUUID(), emptyAliasFilter);
            }
        } else {
            // Apply the query as an alias filter
            AliasFilter aliasFilter = new AliasFilter(queryBuilder, Strings.EMPTY_ARRAY);
            for (Index dataStreamIndex : dataStream.getIndices()) {
                aliasFilters.put(dataStreamIndex.getUUID(), aliasFilter);
            }
        }

        final int shardsWithPrimariesAssigned = assignedPrimaries;
        executeCanMatchPhase(lookup, shardsIter, searchRequest, contextProvider, aliasFilters,
            (searchShardIterators, requests) -> {
                int skippedShards = 0;
                int nonSkippedShards = 0;
                for (List<SearchShardIterator> updatedShardIterators : searchShardIterators.values()) {
                    for (SearchShardIterator shardIterator : updatedShardIterators) {
                        if (shardIterator.skip()) {
                            skippedShards++;
                        } else {
                            nonSkippedShards++;
                        }
                    }
                }

                if (canFilterDataStreamIndices) {
                    assertThat(nonSkippedShards, equalTo(1));
                    assertThat(skippedShards, equalTo(shardsIter.size() - 1));
                    assertThat(requests.size(), equalTo(0));
                } else {
                    // The query included a range within the data stream index timestamp range
                    assertThat(skippedShards, equalTo(0));
                    assertThat(nonSkippedShards, equalTo(shardsIter.size()));
                    // If the shards aren't assigned we don't trigger the can match request
                    assertThat(requests.size(),  equalTo(shardsWithPrimariesAssigned));
                }
            }
        );
    }

    public void testApplyCanMatchOnCoordinatorWithMultipleIndices() throws Exception {
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));

        Index regularIndex = new Index("documents", UUIDs.base64UUID());
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());
        DataStream dataStream =
            new DataStream("mydata", new DataStream.TimestampField("@timestamp"), List.of(dataStreamIndex1, dataStreamIndex2));

        OriginalIndices originalIndices =
            new OriginalIndices(new String[]{regularIndex.getName(), dataStream.getName()}, SearchRequest.DEFAULT_INDICES_OPTIONS);

        GroupShardsIterator<SearchShardIterator> shardsIter = groupShardsIterator(
            getShardsIter(regularIndex, originalIndices, 2, randomBoolean(), primaryNode, replicaNode),
            getUnassignedShardIter(dataStreamIndex1, originalIndices, 2),
            getUnassignedShardIter(dataStreamIndex2, originalIndices, 2)
        );

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(regularIndex.getName(), "documents");
        searchRequest.allowPartialSearchResults(true);

        long indexMinTimestamp = 10;
        long indexMaxTimestamp = 20;
        FakeCoordinatorRewriteContextProvider contextProvider = new FakeCoordinatorRewriteContextProvider();
        // The timestamp ranges of the data stream indices are outside of the query scope
        for (Index dataStreamIndex : dataStream.getIndices()) {
            contextProvider.addIndex(dataStreamIndex, dataStream.getTimeStampField().getName(), indexMinTimestamp, indexMaxTimestamp);
        }

        // We query a range outside of the timestamp range covered by both datastream indices
        RangeQueryBuilder queryBuilder = new RangeQueryBuilder("@timestamp");
        queryBuilder.from(indexMaxTimestamp + 1);
        queryBuilder.to(indexMaxTimestamp + 2);

        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        if (randomBoolean()) {
            // Apply the query on the request body
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
            searchSourceBuilder.query(queryBuilder);
            searchRequest.source(searchSourceBuilder);

            AliasFilter emptyAliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
            aliasFilters.put(dataStreamIndex1.getUUID(), emptyAliasFilter);
            aliasFilters.put(dataStreamIndex2.getUUID(), emptyAliasFilter);
            aliasFilters.put(regularIndex.getUUID(), emptyAliasFilter);

        } else {
            // Apply the query as an alias filter
            AliasFilter aliasFilter = new AliasFilter(queryBuilder, Strings.EMPTY_ARRAY);
            aliasFilters.put(dataStreamIndex1.getUUID(), aliasFilter);
            aliasFilters.put(dataStreamIndex2.getUUID(), aliasFilter);
            aliasFilters.put(regularIndex.getUUID(), new AliasFilter(null, Strings.EMPTY_ARRAY));
        }

        executeCanMatchPhase(lookup, shardsIter, searchRequest, contextProvider, aliasFilters,
            (searchShardIterators, requests) -> {
                for (SearchShardIterator searchShardIterator : searchShardIterators.get(dataStreamIndex1)) {
                    assertTrue(searchShardIterator.skip());
                }
                for (Index dataStreamIndex : dataStream.getIndices()) {
                    for (SearchShardIterator searchShardIterator : searchShardIterators.get(dataStreamIndex)) {
                        assertTrue(searchShardIterator.skip());
                    }
                }

                // All requests that were sent are for the regular index that cannot be pre-filtered
                for (ShardSearchRequest request : requests) {
                    assertThat(request.shardId().getIndex(), equalTo(regularIndex));
                }
            }
        );
    }

    private void executeCanMatchPhase(Map<String, Transport.Connection> lookup,
                                      GroupShardsIterator<SearchShardIterator> shardsIter,
                                      SearchRequest searchRequest,
                                      FakeCoordinatorRewriteContextProvider contextProvider,
                                      Map<String, AliasFilter> aliasFilters,
                                      BiConsumer<Map<Index, List<SearchShardIterator>>,
                                          List<ShardSearchRequest>> phaseResultsConsumer) throws Exception {
        // We respond by default that the query can match
        final List<ShardSearchRequest> requests = Collections.synchronizedList(new ArrayList<>());
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                requests.add(request);
                listener.onResponse(new SearchService.CanMatchResponse(true, null));
            }
        };

        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            aliasFilters,
            Collections.emptyMap(),
            Collections.emptyMap(),
            EsExecutors.newDirectExecutorService(),
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            (iter) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    result.set(iter);
                    latch.countDown();
                }
            },
            SearchResponse.Clusters.EMPTY,
            contextProvider);

        canMatchPhase.start();
        latch.await();

        GroupShardsIterator<SearchShardIterator> updatedShardIterators = result.get();
        Map<Index, List<SearchShardIterator>> shardIteratorsGroupedByIndex = new HashMap<>();
        for (SearchShardIterator searchShardIterator : updatedShardIterators) {
            Index index = searchShardIterator.shardId().getIndex();
            shardIteratorsGroupedByIndex.computeIfAbsent(index, k -> new ArrayList<>()).add(searchShardIterator);
        }

        phaseResultsConsumer.accept(shardIteratorsGroupedByIndex, requests);
    }

    private List<SearchShardIterator> getUnassignedShardIter(Index index, OriginalIndices originalIndices, int numShards) {
        return getShardsIter(index, originalIndices, numShards, false, null, null);
    }

    private GroupShardsIterator<SearchShardIterator> groupShardsIterator(List<SearchShardIterator>... iterators) {
        List<SearchShardIterator> searchShardIterators = new ArrayList<>();
        for (List<SearchShardIterator> iterator : iterators) {
            searchShardIterators.addAll(iterator);
        }
        return GroupShardsIterator.sortAndCreate(searchShardIterators);
    }

    private static class FakeCoordinatorRewriteContextProvider implements CoordinatorRewriteContextProvider {
        private final Map<Index, FieldDataProvider> indices = new HashMap<>();

        private void addIndex(Index index, String fieldName, long minTimeStamp, long maxTimestamp) {
            if (indices.put(index, new FakeFieldDataProvider(fieldName, minTimeStamp, maxTimestamp)) != null) {
                throw new IllegalArgumentException("Index " + index + " was already defined");
            }
        }

        static class FakeFieldDataProvider implements FieldDataProvider {
            private final String fieldName;
            private final long minTimestamp;
            private final long maxTimestamp;

            public FakeFieldDataProvider(String fieldName, long minTimestamp, long maxTimestamp) {
                this.fieldName = fieldName;
                this.minTimestamp = minTimestamp;
                this.maxTimestamp = maxTimestamp;
            }

            @Override
            public Optional<CoordinatorRewriteContext.ConstantField> getField(String fieldName) {
                if (this.fieldName.equals(fieldName)) {
                    final byte[] encodedMinTimestamp = new byte[Long.BYTES];
                    LongPoint.encodeDimension(minTimestamp, encodedMinTimestamp, 0);

                    final byte[] encodedMaxTimestamp = new byte[Long.BYTES];
                    LongPoint.encodeDimension(maxTimestamp, encodedMaxTimestamp, 0);
                    DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(fieldName,
                        DateFieldMapper.Resolution.MILLISECONDS);
                    return Optional.of(new CoordinatorRewriteContext.ConstantField(fieldName, encodedMinTimestamp, encodedMaxTimestamp
                        , fieldType));
                }

                return Optional.empty();
            }
        }

        @Override
        public Optional<CoordinatorRewriteContext> getCoordinatorRewriteContext(Index index) {
            FieldDataProvider fieldDataProvider = indices.get(index);
            if (fieldDataProvider == null) {
                return Optional.empty();
            }

            CoordinatorRewriteContext context = new CoordinatorRewriteContext(mock(NamedXContentRegistry.class),
                mock(NamedWriteableRegistry.class),
                mock(Client.class),
                System::currentTimeMillis,
                fieldDataProvider);

            return Optional.of(context);
        }
    }
}
