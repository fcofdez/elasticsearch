/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportRepositoriesStatsAction extends TransportNodesAction<RepositoriesNodesStatsRequest,
                                                                           RepositoriesNodesStatsResponse,
                                                                           RepositoriesNodeStatsRequest,
                                                                           RepositoriesNodeStatsResponse> {

    private final RepositoriesService repositoriesService;

    public TransportRepositoriesStatsAction(String actionName,
                                            ThreadPool threadPool,
                                            ClusterService clusterService,
                                            TransportService transportService,
                                            ActionFilters actionFilters,
                                            Writeable.Reader<RepositoriesNodesStatsRequest> request,
                                            Writeable.Reader<RepositoriesNodeStatsRequest> nodeRequest,
                                            String nodeExecutor,
                                            Class<RepositoriesNodeStatsResponse> repositoriesNodeStatsResponseClass,
                                            RepositoriesService repositoriesService) {
        super(actionName, threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor, repositoriesNodeStatsResponseClass);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected RepositoriesNodesStatsResponse newResponse(RepositoriesNodesStatsRequest request,
                                                         List<RepositoriesNodeStatsResponse> repositoriesNodeStatsResponses,
                                                         List<FailedNodeException> failures) {
        return new RepositoriesNodesStatsResponse(clusterService.getClusterName(), repositoriesNodeStatsResponses, failures);
    }

    @Override
    protected RepositoriesNodeStatsRequest newNodeRequest(RepositoriesNodesStatsRequest request) {
        return new RepositoriesNodeStatsRequest();
    }

    @Override
    protected RepositoriesNodeStatsResponse newNodeResponse(StreamInput in) throws IOException {
        return new RepositoriesNodeStatsResponse(in);
    }

    @Override
    protected RepositoriesNodeStatsResponse nodeOperation(RepositoriesNodeStatsRequest request, Task task) {
        return new RepositoriesNodeStatsResponse(clusterService.localNode(), repositoriesService.repositoriesStats());
    }
}
