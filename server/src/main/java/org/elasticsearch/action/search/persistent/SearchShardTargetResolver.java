/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;

public interface SearchShardTargetResolver {
    SearchShardIterator resolve(SearchShard shardSearchTarget, OriginalIndices originalIndices);

    class DefaultSearchShardTargetResolver implements SearchShardTargetResolver {
        private final ClusterService clusterService;

        public DefaultSearchShardTargetResolver(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public SearchShardIterator resolve(SearchShard shardSearchTarget, OriginalIndices originalIndices) {
            final ShardId shardId = shardSearchTarget.getShardId();
            // TODO: Throw when index does not exist

            final IndexRoutingTable indexShardRoutingTable = clusterService.state().getRoutingTable().index(shardId.getIndex());
            final IndexShardRoutingTable shardRoutingTable = indexShardRoutingTable.shard(shardId.getId());

            return new SearchShardIterator(null, shardId, shardRoutingTable.activeShards(), originalIndices);
        }
    }
}
