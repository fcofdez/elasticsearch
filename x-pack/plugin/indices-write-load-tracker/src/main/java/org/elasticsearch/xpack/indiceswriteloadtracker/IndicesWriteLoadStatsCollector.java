/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.indiceswriteloadtracker.IndicesWriteLoadTrackerPlugin.currentThreadIsWriterLoadCollectorThreadOrTestThread;

class IndicesWriteLoadStatsCollector implements IndexEventListener {
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeInNanosSupplier;
    private final Set<IndexShard> dataStreamWriteShards = ConcurrentCollections.newConcurrentSet();

    private long latestSampleTimeInNanos;

    IndicesWriteLoadStatsCollector(ClusterService clusterService, LongSupplier relativeTimeInNanosSupplier) {
        this.clusterService = clusterService;
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.latestSampleTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
    }

    void collectWriteLoadStats() {
        assert currentThreadIsWriterLoadCollectorThreadOrTestThread() : Thread.currentThread().getName();

        cleanRolledOverIndices();

        final long relativeTimeInNanos = relativeTimeInNanosSupplier.getAsLong();
        final long totalTimeInNanos = relativeTimeInNanos - latestSampleTimeInNanos;
        latestSampleTimeInNanos = relativeTimeInNanos;

        for (IndexShard indexShard : dataStreamWriteShards) {
            indexShard.recordWriteLoad(totalTimeInNanos);
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
        final var indicesLookup = clusterService.state().metadata().getIndicesLookup();
        dataStreamWriteShards.removeIf(shard -> {
            final var shardId = shard.shardId();
            return isDataStreamWriteIndex(shardId.getIndex(), indicesLookup) == false;
        });
    }
}
