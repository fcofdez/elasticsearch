/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.metering;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesRepositoryMeteringAction extends TransportNodesAction<NodesRepositoriesCountersRequest,
                                                                                 NodesRepositoriesCountersResponse,
                                                                                 NodeRepositoriesCountersRequest,
                                                                                 NodeRepositoriesCountersResponse> {

    public TransportNodesRepositoryMeteringAction(String actionName,
                                                  ThreadPool threadPool,
                                                  ClusterService clusterService,
                                                  TransportService transportService,
                                                  ActionFilters actionFilters,
                                                  Writeable.Reader<NodesRepositoriesCountersRequest> request,
                                                  Writeable.Reader<NodeRepositoriesCountersRequest> nodeRequest,
                                                  String nodeExecutor,
                                                  Class<NodeRepositoriesCountersResponse> nodeRepositoriesCountersClass) {
        super(actionName, threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor, nodeRepositoriesCountersClass);
    }

    @Override
    protected NodesRepositoriesCountersResponse newResponse(NodesRepositoriesCountersRequest request, List<NodeRepositoriesCountersResponse> nodeRepositoriesCounterResponses, List<FailedNodeException> failures) {
        return new NodesRepositoriesCountersResponse(clusterService.getClusterName(), nodeRepositoriesCounterResponses, failures);
    }

    @Override
    protected NodeRepositoriesCountersRequest newNodeRequest(NodesRepositoriesCountersRequest request) {
        return new NodeRepositoriesCountersRequest();
    }

    @Override
    protected NodeRepositoriesCountersResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeRepositoriesCountersResponse(in);
    }

    @Override
    protected NodeRepositoriesCountersResponse nodeOperation(NodeRepositoriesCountersRequest request, Task task) {
        return new NodeRepositoriesCountersResponse(clusterService.localNode());
    }
}
