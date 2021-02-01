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

package org.elasticsearch.rest.action.search.persistent;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchAction;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.search.RestSearchAction.parseSearchRequest;

public class RestSubmitPersistentSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "submit_persistent_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_persistent_search"),
            new Route(POST, "/{index}/_persistent_search")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser ->
            parseSearchRequest(searchRequest, request, parser, client.getNamedWriteableRegistry(), setSize));

        return channel -> {
            RestStatusToXContentListener<SubmitPersistentSearchResponse> listener = new RestStatusToXContentListener<>(channel);
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SubmitPersistentSearchAction.INSTANCE, searchRequest, listener);
        };
    }
}
