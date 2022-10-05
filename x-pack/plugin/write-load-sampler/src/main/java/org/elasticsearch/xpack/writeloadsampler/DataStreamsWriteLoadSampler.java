/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.writeloadsampler.DataStreamsWriteLoadSamplerPlugin.currentThreadIsWriterLoadSamplerThreadOrTestThread;

class DataStreamsWriteLoadSampler implements IndexEventListener {
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final Set<IndexShard> dataStreamWriteShards = ConcurrentCollections.newConcurrentSet();

    private long latestSampleTimeInNanos;
    private ClusterState latestKnownClusterState;

    DataStreamsWriteLoadSampler(Supplier<ClusterState> clusterStateSupplier, LongSupplier relativeTimeInNanosSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.latestSampleTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
    }

    void sampleWriteLoadStats() {
        assert currentThreadIsWriterLoadSamplerThreadOrTestThread() : Thread.currentThread().getName();

        cleanRolledOverIndices();

        final long relativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
        final long timeSinceLastSampleInNanos = relativeTimeInNanos - latestSampleTimeInNanos;
        latestSampleTimeInNanos = relativeTimeInNanos;

        for (IndexShard indexShard : dataStreamWriteShards) {
            indexShard.recordWriteLoad(timeSinceLastSampleInNanos);
        }
    }

    private static boolean isDataStreamWriteIndex(Index index, Map<String, IndexAbstraction> indicesLookup) {
        final var indexAbstraction = indicesLookup.get(index.getName());
        if (indexAbstraction == null) {
            return false;
        }

        final var parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null && index.equals(parentDataStream.getWriteIndex());
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.isDataStreamIndex()) {
            dataStreamWriteShards.add(indexShard);
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        dataStreamWriteShards.remove(indexShard);
    }

    private void cleanRolledOverIndices() {
        final ClusterState clusterState = clusterStateSupplier.get();
        if (latestKnownClusterState == null || clusterState.supersedes(latestKnownClusterState)) {
            final var indicesLookup = clusterState.metadata().getIndicesLookup();
            dataStreamWriteShards.removeIf(shard -> {
                final var shardId = shard.shardId();
                return isDataStreamWriteIndex(shardId.getIndex(), indicesLookup) == false;
            });
            latestKnownClusterState = clusterState;
        }
    }
}
