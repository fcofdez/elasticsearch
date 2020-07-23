/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.metering.action.RepositoriesNodesStatsRequest;
import org.elasticsearch.xpack.metering.action.RepositoriesStatsAction;

import java.util.List;

public class RestRepositoriesStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "repositories_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_nodes/{nodeId}/_repositories_stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        RepositoriesNodesStatsRequest repositoriesNodesStatsRequest = new RepositoriesNodesStatsRequest(nodesIds);
        return channel -> client.execute(RepositoriesStatsAction.INSTANCE, repositoriesNodesStatsRequest, new RestToXContentListener<>(channel));
    }
}
