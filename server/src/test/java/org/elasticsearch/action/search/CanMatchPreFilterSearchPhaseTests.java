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
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.FieldDataProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

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
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
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
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
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
            SearchAsyncActionTests.getShardsIter("idx", originalIndices, 4096, randomBoolean(), primaryNode, replicaNode);
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
            GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("logs",
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
            GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("logs",
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

    public void testPreFilter() throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();

        final AtomicBoolean wasCanMatchRequestSent = new AtomicBoolean(false);
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                wasCanMatchRequestSent.set(true);
            }
        };

        final String dataStreamIndexName = ".ds-mydata0001";

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(dataStreamIndexName,
            new OriginalIndices(new String[]{dataStreamIndexName}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(".ds-mydata0001");
        searchRequest.allowPartialSearchResults(true);
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        RangeQueryBuilder queryBuilder = new RangeQueryBuilder("@timestamp");
        queryBuilder.from(10);
        queryBuilder.to(20);
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);

        // TODO: add alias filters
        final String regularIndexName = "data";
        final long minTimestamp = Long.MIN_VALUE;
        final long maxTimestamp = Long.MIN_VALUE + 1;
        CoordinatorRewriteContextProvider provider = (index) -> {
            if (index.getName().equals(dataStreamIndexName)) {
                FieldDataProvider f = fieldName -> {
                    if (fieldName.equals("@timestamp")) {
                        final byte[] encodedMaxTimestamp = new byte[Long.BYTES];
                        LongPoint.encodeDimension(maxTimestamp, encodedMaxTimestamp, 0);
                        final byte[] encodedMinTimestamp = new byte[Long.BYTES];
                        LongPoint.encodeDimension(minTimestamp, encodedMinTimestamp, 0);
                        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(fieldName,
                            DateFieldMapper.Resolution.MILLISECONDS);
                        return Optional.of(new CoordinatorRewriteContext.ConstantField(fieldName, encodedMinTimestamp, encodedMaxTimestamp
                            , fieldType));
                    }
                    return Optional.empty();
                };

                return Optional.of(new CoordinatorRewriteContext(null, null, null, System::currentTimeMillis, f));
            }

            return Optional.empty();
        };

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
                }
            }, SearchResponse.Clusters.EMPTY, provider);

        canMatchPhase.start();
        latch.await();

        assertFalse(wasCanMatchRequestSent.get());
        // Special case where none of the shards is a hit
        assertFalse(result.get().get(0).skip());
        assertTrue(result.get().get(1).skip());
    }

    public void testPreFilterWithMultipleIndices() throws Exception {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();
        Index regularIndex = new Index("documents", UUIDs.base64UUID());
        Index dataStreamIndex1 = new Index(".ds-mydata0001", UUIDs.base64UUID());
        Index dataStreamIndex2 = new Index(".ds-mydata0002", UUIDs.base64UUID());


        final Map<DiscoveryNode, List<ShardSearchRequest>> requestMap = ConcurrentCollections.newConcurrentMap();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                requestMap.computeIfAbsent(connection.getNode(), k -> new ArrayList<>()).add(request);
                listener.onResponse(new SearchService.CanMatchResponse(true, null));
            }
        };

        OriginalIndices originalIndices = new OriginalIndices(new String[]{"mydata", "documents"}, SearchRequest.DEFAULT_INDICES_OPTIONS);
        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        List<SearchShardIterator> ss = getShardsIter(regularIndex,
            originalIndices,
            2, randomBoolean(), primaryNode, replicaNode);
        // non allocated shards
        List<SearchShardIterator> shardsIter1 = getShardsIter(dataStreamIndex1, originalIndices, 2, false, null, null);
        // non allocated shards
        List<SearchShardIterator> shardsIter2 = getShardsIter(dataStreamIndex2, originalIndices, 2, false, null, null);

        List<SearchShardIterator> s = new ArrayList<>(ss);
        s.addAll(shardsIter1);
        s.addAll(shardsIter2);
        GroupShardsIterator<SearchShardIterator> shardsIter = GroupShardsIterator.sortAndCreate(s);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("mydata", "documents");
        searchRequest.allowPartialSearchResults(true);
//        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        RangeQueryBuilder queryBuilder = new RangeQueryBuilder("@timestamp");
        queryBuilder.from(10);
        queryBuilder.to(20);
//        searchSourceBuilder.query(queryBuilder);
//        searchRequest.source(searchSourceBuilder);
        AliasFilter aliasFilter = new AliasFilter(queryBuilder, Strings.EMPTY_ARRAY);
        Map<String, AliasFilter> aliasFilters = new HashMap<>();
        aliasFilters.put(dataStreamIndex1.getUUID(), aliasFilter);
        aliasFilters.put(dataStreamIndex2.getUUID(), aliasFilter);
        aliasFilters.put(regularIndex.getUUID(), new AliasFilter(null, Strings.EMPTY_ARRAY));


        final long minTimestamp = Long.MIN_VALUE;
        final long maxTimestamp = Long.MIN_VALUE + 1;
        CoordinatorRewriteContextProvider provider = (index) -> {
            String name = index.getName();
            if (name.equals(dataStreamIndex1.getName()) || name.equals(dataStreamIndex2.getName())) {
                FieldDataProvider f = fieldName -> {
                    if (fieldName.equals("@timestamp")) {
                        final byte[] encodedMaxTimestamp = new byte[Long.BYTES];
                        LongPoint.encodeDimension(maxTimestamp, encodedMaxTimestamp, 0);
                        final byte[] encodedMinTimestamp = new byte[Long.BYTES];
                        LongPoint.encodeDimension(minTimestamp, encodedMinTimestamp, 0);
                        DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(fieldName,
                            DateFieldMapper.Resolution.MILLISECONDS);
                        return Optional.of(new CoordinatorRewriteContext.ConstantField(fieldName, encodedMinTimestamp, encodedMaxTimestamp
                            , fieldType));
                    }
                    return Optional.empty();
                };

                return Optional.of(new CoordinatorRewriteContext(null, null, null, System::currentTimeMillis, f));
            }

            return Optional.empty();
        };

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
            provider);

        canMatchPhase.start();
        latch.await();

        GroupShardsIterator<SearchShardIterator> searchShardIterators = result.get();
        Map<Index, List<SearchShardIterator>> iterators = new HashMap<>();
        for (SearchShardIterator searchShardIterator : searchShardIterators) {
            iterators.computeIfAbsent(searchShardIterator.shardId().getIndex(), k -> new ArrayList<>()).add(searchShardIterator);
        }

        for (SearchShardIterator searchShardIterator : iterators.get(dataStreamIndex1)) {
            assertTrue(searchShardIterator.skip());
        }

        for (SearchShardIterator searchShardIterator : iterators.get(dataStreamIndex2)) {
            assertTrue(searchShardIterator.skip());
        }

        for (SearchShardIterator searchShardIterator : iterators.get(regularIndex)) {
            assertFalse(searchShardIterator.skip());
        }

        // All request were for the regular index
        for (List<ShardSearchRequest> requests : requestMap.values()) {
            for (ShardSearchRequest request : requests) {
                assertThat(request.shardId().getIndex(), equalTo(regularIndex));
            }
        }
    }

    static List<SearchShardIterator> getShardsIter(Index index, OriginalIndices originalIndices, int numShards,
                                                   boolean doReplicas, DiscoveryNode primaryNode, DiscoveryNode replicaNode) {
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ArrayList<ShardRouting> started = new ArrayList<>();
            ArrayList<ShardRouting> initializing = new ArrayList<>();
            ArrayList<ShardRouting> unassigned = new ArrayList<>();

            ShardRouting routing = ShardRouting.newUnassigned(new ShardId(index, i), true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
            if (primaryNode != null) {
                routing = routing.initialize(primaryNode.getId(), i + "p", 0);
                routing = routing.moveToStarted();
                started.add(routing);
            }
            if (primaryNode != null && doReplicas) {
                routing = ShardRouting.newUnassigned(new ShardId(index, i), false,
                    RecoverySource.PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
                if (replicaNode != null) {
                    routing = routing.initialize(replicaNode.getId(), i + "r", 0);
                    if (randomBoolean()) {
                        routing = routing.moveToStarted();
                        started.add(routing);
                    } else {
                        initializing.add(routing);
                    }
                } else {
                    unassigned.add(routing); // unused yet
                }
            }
            Collections.shuffle(started, random());
            started.addAll(initializing);
            list.add(new SearchShardIterator(null, new ShardId(index, i), started, originalIndices));
        }
        return list;
    }
}
