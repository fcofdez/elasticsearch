/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class DataStreamsWriteLoadSamplerTests extends ESTestCase {
    private TestThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testOnlyDataStreamWriteIndicesLoadIsSampled() throws Exception {
        final String dataStreamName = "myds";
        final String writeIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1, 0);
        final String regularIndex = "index2";

        final var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata(writeIndex), false)
                    .put(indexMetadata(regularIndex), false)
                    .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(index(writeIndex))))
                    .build()
            )
            .nodes(discoveryNodes())
            .build();
        final AtomicReference<ClusterState> clusterStateProvider = new AtomicReference<>(clusterState);
        final var dataStreamsWriteLoadSampler = new DataStreamsWriteLoadSampler(clusterStateProvider::get, () -> 0L);

        final var dataStreamShard = createInstrumentedIndexShard(writeIndex, true);
        final var regularShard = createInstrumentedIndexShard(regularIndex, false);

        dataStreamsWriteLoadSampler.afterIndexShardStarted(dataStreamShard);
        dataStreamsWriteLoadSampler.afterIndexShardStarted(regularShard);

        final int numberOfSamplingEvents = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfSamplingEvents; i++) {
            dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        }

        assertThat(regularShard.numberOfRecordWriteLoadCalls, equalTo(0));
        assertThat(dataStreamShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEvents));
    }

    public void testWriteLoadIsNotSampledAfterShardIsClosed() throws Exception {
        final String dataStreamName = "myds";
        final String writeIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1, 0);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata(writeIndex), false)
                    .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(index(writeIndex))))
                    .build()
            )
            .nodes(discoveryNodes())
            .build();
        final AtomicReference<ClusterState> clusterStateProvider = new AtomicReference<>(clusterState);
        final var dataStreamsWriteLoadSampler = new DataStreamsWriteLoadSampler(clusterStateProvider::get, () -> 0L);

        final var dataStreamShard = createInstrumentedIndexShard(writeIndex, true);
        dataStreamsWriteLoadSampler.afterIndexShardStarted(dataStreamShard);

        final int numberOfSamplingEventsBeforeClosingTheShard = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfSamplingEventsBeforeClosingTheShard; i++) {
            dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        }

        assertThat(dataStreamShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEventsBeforeClosingTheShard));

        dataStreamsWriteLoadSampler.afterIndexShardClosed(dataStreamShard.shardId(), dataStreamShard, Settings.EMPTY);

        final int numberOfSamplingEventsAfterShardIsClosed = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfSamplingEventsAfterShardIsClosed; i++) {
            dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        }
        assertThat(dataStreamShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEventsBeforeClosingTheShard));
    }

    public void testRolledOverDataStreamShardsAreNotSampled() throws Exception {
        final String dataStreamName = "myds";
        final String writeIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1, 0);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata(writeIndex), false)
                    .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(index(writeIndex))))
                    .build()
            )
            .nodes(discoveryNodes())
            .build();
        final AtomicReference<ClusterState> clusterStateProvider = new AtomicReference<>(clusterState);
        final var dataStreamsWriteLoadSampler = new DataStreamsWriteLoadSampler(clusterStateProvider::get, () -> 0L);

        final var dataStreamShard = createInstrumentedIndexShard(writeIndex, true);
        dataStreamsWriteLoadSampler.afterIndexShardStarted(dataStreamShard);

        final int numberOfSamplingEventsBeforeRollover = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfSamplingEventsBeforeRollover; i++) {
            dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        }

        assertThat(dataStreamShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEventsBeforeRollover));

        final String newWriteIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 2, 0);
        final ClusterState updatedClusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata(writeIndex), false)
                    .put(indexMetadata(newWriteIndex), false)
                    .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(index(writeIndex), index(newWriteIndex))))
                    .build()
            )
            .incrementVersion()
            .build();
        clusterStateProvider.set(updatedClusterState);

        final var newWriteShard = createInstrumentedIndexShard(newWriteIndex, true);
        dataStreamsWriteLoadSampler.afterIndexShardStarted(newWriteShard);

        final int numberOfSamplingEventsAfterRollover = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfSamplingEventsAfterRollover; i++) {
            dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        }

        assertThat(dataStreamShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEventsBeforeRollover));
        assertThat(newWriteShard.numberOfRecordWriteLoadCalls, equalTo(numberOfSamplingEventsAfterRollover));
    }

    private DiscoveryNodes discoveryNodes() {
        final DiscoveryNode node = new DiscoveryNode(
            "node_0",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.roles()),
            Version.CURRENT
        );
        return DiscoveryNodes.builder().add(node).localNodeId(node.getId()).masterNodeId(node.getId()).build();
    }

    private static IndexMetadata indexMetadata(String indexName) {
        return IndexMetadata.builder(indexName).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
    }

    private Index index(String indexName) {
        return new Index(indexName, "__na__");
    }

    private InstrumentedIndexShard createInstrumentedIndexShard(String indexName, boolean isDataStream) throws Exception {
        final ShardId shardId = new ShardId(index(indexName), 0);
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final NodeEnvironment.DataPath dataPath = new NodeEnvironment.DataPath(createTempDir());
        final ShardPath shardPath = new ShardPath(false, dataPath.resolve(shardId), dataPath.resolve(shardId), shardId);
        return new InstrumentedIndexShard(shardRouting, indexSettings, shardPath, threadPool, isDataStream);
    }

    static class InstrumentedIndexShard extends IndexShard {
        private final boolean isDataStream;
        private int numberOfRecordWriteLoadCalls = 0;

        InstrumentedIndexShard(
            ShardRouting shardRouting,
            IndexSettings indexSettings,
            ShardPath shardPath,
            ThreadPool threadPool,
            boolean isDataStream
        ) throws IOException {
            super(
                shardRouting,
                indexSettings,
                shardPath,
                mock(Store.class),
                null,
                null,
                null,
                null,
                mock(EngineFactory.class),
                null,
                null,
                threadPool,
                null,
                null,
                List.of(),
                List.of(),
                null,
                mock(RetentionLeaseSyncer.class),
                null,
                mock(IndexStorePlugin.SnapshotCommitSupplier.class)
            );
            this.isDataStream = isDataStream;
        }

        @Override
        public boolean isDataStreamIndex() {
            return isDataStream;
        }

        @Override
        public void recordWriteLoad(long samplingTimeInNanos) {
            numberOfRecordWriteLoadCalls++;
        }
    }
}
