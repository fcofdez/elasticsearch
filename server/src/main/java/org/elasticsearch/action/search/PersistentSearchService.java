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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.search.persistent.AsyncPersistentSearch;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchRequest;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchResponse;
import org.elasticsearch.search.persistent.PersistentSearchResponse;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.persistent.PersistentSearchStorageService;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;

public class PersistentSearchService {
    private final SearchService searchService;
    private final SearchPhaseController searchPhaseController;
    private final PersistentSearchStorageService searchStorageService;
    private final Executor executor;
    private final TransportService transportService;

    public PersistentSearchService(SearchService searchService,
                                   SearchPhaseController searchPhaseController,
                                   PersistentSearchStorageService searchStorageService,
                                   Executor executor,
                                   TransportService transportService) {
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
        this.searchStorageService = searchStorageService;
        this.executor = executor;
        this.transportService = transportService;
    }

    public void executeAsyncQueryPhase(ExecutePersistentQueryFetchRequest request,
                                       SearchShardTask task, ActionListener<ExecutePersistentQueryFetchResponse> listener) {
        StepListener<SearchPhaseResult> queryListener = new StepListener<>();
        StepListener<Void> storeListener = new StepListener<>();

        final ShardSearchRequest shardSearchRequest = request.getShardSearchRequest();
        queryListener.whenComplete(result -> {
            final SearchResponse searchResponse =
                convertToSearchResponse((QueryFetchSearchResult) result, searchService.aggReduceContextBuilder(shardSearchRequest.source()));
            final ShardId shardId = request.getShardSearchRequest().shardId();
            final String id = PersistentSearchResponse.generatePartialResultIdId(request.getAsyncSearchId(), shardId);
            final PersistentSearchResponse persistentSearchResponse = new PersistentSearchResponse(id, searchResponse);
            searchStorageService.storeResult(persistentSearchResponse, storeListener);
        }, listener::onFailure);

        storeListener.whenComplete(r -> listener.onResponse(new ExecutePersistentQueryFetchResponse()), listener::onFailure);

        searchService.executeQueryPhase(shardSearchRequest, false, task, queryListener);
    }

    public void executePartialReduce(ReducePartialPersistentSearchRequest request,
                                     SearchShardTask task,
                                     ActionListener<ReducePartialPersistentSearchResponse> listener) {
        // we need to use versioning for SearchResponse (so we avoid conflicting operations)
        // store the number of reduced shards (this would allow to run the last reduction)
        final String searchId = request.getSearchId();
        StepListener<PersistentSearchResponse> getSearchResultListener = new StepListener<>();
        StepListener<SearchResponse> reduceListener = new StepListener<>();

        getSearchResultListener.whenComplete((persistentSearchResponse -> {
            try {
                // TODO: Extract to a class that keeps track of versioning and base search response
                final SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                    SearchService.DEFAULT_FROM,
                    SearchService.DEFAULT_SIZE,
                    SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
                    // TODO: Check why this is important, we might need to pass the values around
                    new TransportSearchAction.SearchTimeProvider(0, 0, () -> 1L),
                    searchPhaseController.getReduceContext(request.getOriginalRequest())
                );

                if (persistentSearchResponse != null) {
                    searchResponseMerger.add(persistentSearchResponse.getSearchResponse());
                }

                runAsync(() -> reduce(searchResponseMerger, request), reduceListener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }), listener::onFailure);

        reduceListener.whenComplete((reducedSearchResponse -> {
            final PersistentSearchResponse persistentSearchResponse =
                new PersistentSearchResponse(request.getSearchId(), reducedSearchResponse);
            searchStorageService.storeResult(persistentSearchResponse, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    listener.onResponse(new ReducePartialPersistentSearchResponse(request.getShardsToReduce()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }), listener::onFailure);

        searchStorageService.getPersistentSearchResult(searchId, getSearchResultListener);
    }

    private SearchResponse reduce(SearchResponseMerger searchResponseMerger, ReducePartialPersistentSearchRequest request) {
        // TODO: Use circuit breaker
        for (SearchShard searchShard : request.getShardsToReduce()) {
            final String partialResultId =
                PersistentSearchResponse.generatePartialResultIdId(request.getSearchId(), searchShard.getShardId());

            try {
                final SearchResponse partialResult
                    = searchStorageService.getPartialResult(partialResultId);
                searchResponseMerger.add(partialResult);
            } catch (Exception e) {
                // Ignore if not exists for now...
            }
        }

        return searchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY);
    }

    private SearchResponse convertToSearchResponse(QueryFetchSearchResult result,
                                                   InternalAggregation.ReduceContextBuilder aggReduceContextBuilder) {
        SearchPhaseController.TopDocsStats topDocsStats = new SearchPhaseController.TopDocsStats(0);
        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase =
            SearchPhaseController.reducedQueryPhase(Collections.singletonList(result),
                emptyList(),
                emptyList(),
                topDocsStats,
                0,
                false,
                aggReduceContextBuilder,
                false);
        final List<FetchSearchResult> fetchResults = Collections.singletonList(result.fetchResult());

        final InternalSearchResponse internalSearchResponse
            = searchPhaseController.merge(false, reducedQueryPhase, fetchResults, fetchResults::get);
        return
            new SearchResponse(internalSearchResponse,
                null,
                1,
                1,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY);
    }

    private <T> void runAsync(CheckedSupplier<T, Exception> executable, ActionListener<T> listener) {
        executor.execute(ActionRunnable.supply(listener, executable));
    }
}
