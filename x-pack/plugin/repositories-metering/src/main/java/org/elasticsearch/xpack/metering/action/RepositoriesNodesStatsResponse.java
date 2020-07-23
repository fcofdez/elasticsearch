/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RepositoriesNodesStatsResponse extends BaseNodesResponse<RepositoriesNodeStatsResponse> implements ToXContentObject {
    public RepositoriesNodesStatsResponse(ClusterName clusterName, List<RepositoriesNodeStatsResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<RepositoriesNodeStatsResponse> readNodesFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        List<RepositoriesNodeStatsResponse> nodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodes.add(new RepositoriesNodeStatsResponse(in));
        }
        return nodes;
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<RepositoriesNodeStatsResponse> nodes) throws IOException {
        out.writeInt(nodes.size());
        for (RepositoriesNodeStatsResponse node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (RepositoriesNodeStatsResponse nodeResponse : getNodes()) {
            nodeResponse.toXContent(builder, params);
        }
        for (FailedNodeException failure : failures()) {
            failure.toXContent(builder, params);
        }
        return builder;
    }
}
