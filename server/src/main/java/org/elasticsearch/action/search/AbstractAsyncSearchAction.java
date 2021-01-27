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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.AsyncShardSearchRequest;
import org.elasticsearch.search.internal.ReducePartialResultsRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractAsyncSearchAction {
    private final String asyncSearchId;
    private final Queue<SearchShard> shardSearchTargetQueue;
    private final int maxConcurrentRequests;
    private final ShardSearchTargetResolver shardSearchTargetResolver;
    private final SearchTransportService searchTransportService;
    private final SearchRequest request;
    private final SearchTask task;
    private final Executor executor;
    private final AtomicInteger runningRequests = new AtomicInteger(0);
    private final Set<SearchShardIterator> runningReduces = ConcurrentCollections.newConcurrentSet();
    private final AtomicInteger pendingReduceCount = new AtomicInteger(0);
    private final Set<SearchShard> pendingReduces = ConcurrentCollections.newConcurrentSet();

    public AbstractAsyncSearchAction(String asyncSearchId,
                                     List<SearchShard> searchShards,
                                     int maxConcurrentRequests,
                                     ShardSearchTargetResolver shardSearchTargetResolver,
                                     SearchTransportService searchTransportService,
                                     SearchRequest request,
                                     SearchTask task,
                                     Executor executor) {
        this.asyncSearchId = asyncSearchId;
        // Use the same order as provided?
        this.shardSearchTargetQueue = new ArrayDeque<>(searchShards);
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.shardSearchTargetResolver = shardSearchTargetResolver;
        this.searchTransportService = searchTransportService;
        this.request = request;
        this.task = task;
        this.executor = executor;
    }

    void run() {
        SearchShard target = null;
        while (runningRequests.incrementAndGet() < maxConcurrentRequests && (target = shardSearchTargetQueue.poll()) != null) {
            final SearchShardIterator searchShardIterator = shardSearchTargetResolver.resolve(target);
            final SearchShard finalTarget = target;
            executeShardSearch(searchShardIterator, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    onShardSuccess(finalTarget);
                }

                @Override
                public void onFailure(Exception e) {
                    runningRequests.decrementAndGet();

                    // Re-execute
                    shardSearchTargetQueue.add(finalTarget);
                    executor.execute(() -> run());
                }
            });
        }
    }

    void onShardSuccess(SearchShard target) {
        runningRequests.decrementAndGet();
        pendingReduces.add(target);
        if (pendingReduceCount.incrementAndGet() == 5) {
            executeReduces(pendingReduces, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    pendingReduces.clear();
                    // is this thread safe?
                    pendingReduceCount.set(0);
                    executor.execute(() -> run());
                }

                @Override
                public void onFailure(Exception e) {
                    // Retry on another node?
                    // Fail the search?
                }
            });
        } else {

            executor.execute(this::run);
        }
    }

    void onAllShardsExecuted() {


    }

    void executeReduces(Set<SearchShard> pendingShardResultsToReduce, ActionListener<Void> listener) {
        final ReducePartialResultsRequest request = new ReducePartialResultsRequest(asyncSearchId, List.of());
        searchTransportService.sendExecutePartialReduce(null, request, task, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

    }

    void executeShardSearch(final SearchShardIterator searchShardIterator, ActionListener<Void> listener) {
        final SearchShardTarget searchShardTarget = searchShardIterator.nextOrNull();
        if (searchShardTarget == null) {
            listener.onFailure(new RuntimeException("Shard not found " + searchShardIterator.shardId()));
            return;
        }

        ShardSearchRequest querySearchRequest = buildShardSearchRequest(searchShardIterator, 0);
        final AsyncShardSearchRequest asyncShardSearchRequest = new AsyncShardSearchRequest(UUIDs.randomBase64UUID(), querySearchRequest);

        // Execute on all shards
        final Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
        searchTransportService.sendAsyncQuery(connection, asyncShardSearchRequest, task, new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse.Empty empty) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                executor.execute(() -> executeShardSearch(searchShardIterator, listener));
            }
        });
    }

    private Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return null;
    }

    public final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        AliasFilter filter = AliasFilter.EMPTY;
        float indexBoost = 1.0f;
        ShardSearchRequest shardRequest = new ShardSearchRequest(shardIt.getOriginalIndices(), request, shardIt.shardId(), shardIndex,
            getNumShards(), filter, indexBoost, getAbsoluteStartMillis(), shardIt.getClusterAlias(),
            shardIt.getSearchContextId(), shardIt.getSearchContextKeepAlive());
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(false);
        return shardRequest;
    }

    int getNumShards() {
        return 1;
    }

    long getAbsoluteStartMillis() {
        return System.currentTimeMillis();
    }

    interface ShardSearchTargetResolver {
        SearchShardIterator resolve(SearchShard shardSearchTarget);
    }
}
