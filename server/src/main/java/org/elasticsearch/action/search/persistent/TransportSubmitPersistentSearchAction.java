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

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.persistent.PersistentSearchId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

public class TransportSubmitPersistentSearchAction extends HandledTransportAction<SearchRequest, SubmitPersistentSearchResponse> {
    private final NodeClient nodeClient;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final RemoteClusterService remoteClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SearchShardTargetResolver searchShardTargetResolver;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportSubmitPersistentSearchAction(TransportService transportService,
                                                 ActionFilters actionFilters,
                                                 NodeClient nodeClient,
                                                 ThreadPool threadPool,
                                                 ClusterService clusterService,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 SearchShardTargetResolver searchShardTargetResolver,
                                                 SearchTransportService searchTransportService) {
        super(SubmitPersistentSearchAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.nodeClient = nodeClient;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.searchShardTargetResolver = searchShardTargetResolver;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SubmitPersistentSearchResponse> listener) {
        SearchTask searchTask = (SearchTask) taskManager.register("transport", SearchAction.INSTANCE.name(), request);
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider =
            new TransportSearchAction.SearchTimeProvider(request.getOrCreateAbsoluteStartMillis(), relativeStartNanos, System::nanoTime);

        final Map<String, OriginalIndices> resolvedIndices
            = remoteClusterService.groupIndices(request.indicesOptions(), request.indices());
        final OriginalIndices localIndices = resolvedIndices.remove(LOCAL_CLUSTER_GROUP_KEY);
        if (resolvedIndices.isEmpty() == false) {
            listener.onFailure(new RuntimeException("Unable to run a CCS under this mode"));
            return;
        }

        final ClusterState clusterState = clusterService.state();
        final Index[] indices =
            indexNameExpressionResolver.concreteIndices(clusterState, localIndices, timeProvider.getAbsoluteStartMillis());

        final List<SearchShard> searchShards = resolveShards(clusterState, indices);

        final String persistentSearchDocId = UUIDs.randomBase64UUID();
        final PersistentSearchId persistentSearchId =
            new PersistentSearchId(persistentSearchDocId, new TaskId(nodeClient.getLocalNodeId(), task.getId()));

        new AsyncPersistentSearch(request,
            persistentSearchDocId,
            searchTask,
            searchShards,
            localIndices,
            1,
            searchShardTargetResolver,
            searchTransportService,
            threadPool,
            connectionProvider(),
            clusterService,
            timeProvider
            ).run();

        listener.onResponse(new SubmitPersistentSearchResponse(persistentSearchId));
    }

    private BiFunction<String, String, Transport.Connection> connectionProvider() {
        return (cluster, nodeId) -> {
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            return searchTransportService.getConnection(null, node);
        };
    }

    private List<SearchShard> resolveShards(ClusterState clusterState, Index[] indices) {
        List<SearchShard> searchShards = new ArrayList<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexMetadata == null) {
                continue;
            }
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                searchShards.add(new SearchShard(null, new ShardId(index, i)));
            }
        }
        return Collections.unmodifiableList(searchShards);
    }
}
