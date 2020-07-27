/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryId;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RepositoriesNodeStatsResponse extends BaseNodeResponse implements ToXContent {

    private final Map<RepositoryId, List<RepositoryStatsSnapshot>> repositoriesStatsById;

    public RepositoriesNodeStatsResponse(DiscoveryNode node, Map<RepositoryId, List<RepositoryStatsSnapshot>> repositoriesStatsById) {
        super(node);
        this.repositoriesStatsById = repositoriesStatsById;
    }

    public RepositoriesNodeStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.repositoriesStatsById = in.readMap(RepositoryId::new, streamInput -> streamInput.readList(RepositoryStatsSnapshot::new));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        DiscoveryNode node = getNode();
        builder.startArray(node.getId());
        for (List<RepositoryStatsSnapshot> repositoryStats : repositoriesStatsById.values()) {
            for (RepositoryStatsSnapshot repositoryStat : repositoryStats) {
                repositoryStat.toXContent(builder, params);
            }
        }
        builder.endArray();
        return builder;
    }
}
