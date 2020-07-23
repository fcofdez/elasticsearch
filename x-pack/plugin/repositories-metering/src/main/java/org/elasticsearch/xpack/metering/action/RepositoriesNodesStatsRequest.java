/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class RepositoriesNodesStatsRequest extends BaseNodesRequest<RepositoriesNodesStatsRequest> {
    public RepositoriesNodesStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public RepositoriesNodesStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public RepositoriesNodesStatsRequest(DiscoveryNode... concreteNodes) {
        super(concreteNodes);
    }
}
