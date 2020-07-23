/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.RepositoryStats;

import java.io.IOException;
import java.util.List;

public class RepositoriesNodeStatsResponse extends BaseNodeResponse implements ToXContent {

    private final List<RepositoryStats> repositoriesStatsArchive;

    public RepositoriesNodeStatsResponse(DiscoveryNode node, List<RepositoryStats> repositoriesStatsArchive) {
        super(node);
        this.repositoriesStatsArchive = repositoriesStatsArchive;
    }

    public RepositoriesNodeStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.repositoriesStatsArchive = in.readList(RepositoryStats::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        DiscoveryNode node = getNode();
        builder.startArray(node.getId());
        for (RepositoryStats repositoryStats : repositoriesStatsArchive) {
            repositoryStats.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
