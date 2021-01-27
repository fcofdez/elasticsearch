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
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.search.PersistentSearchStorageService;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.AsyncShardSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ReducePartialResultsRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.TransportResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Collections.emptyList;

class PersistentSearchService {
    private final SearchService searchService;
    private final SearchPhaseController searchPhaseController;
    private final PersistentSearchStorageService searchStorageService;
    private final Executor executor;

    PersistentSearchService(SearchService searchService,
                            SearchPhaseController searchPhaseController,
                            PersistentSearchStorageService searchStorageService,
                            Executor executor) {
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
        this.searchStorageService = searchStorageService;
        this.executor = executor;
    }

    public void executeAsyncQueryPhase(AsyncShardSearchRequest request, boolean keepStatesInContext,
                                       SearchShardTask task, ActionListener<TransportResponse.Empty> listener) {
        StepListener<SearchPhaseResult> queryListener = new StepListener<>();
        StepListener<Void> storeListener = new StepListener<>();

        final ShardSearchRequest shardSearchRequest = request.getShardSearchRequest();
        queryListener.whenComplete(result -> {
            final SearchResponse searchResponse =
                convertToSearchResponse((QueryFetchSearchResult) result, searchService.aggReduceContextBuilder(shardSearchRequest.source()));
            final PersistentSearchResponse persistentSearchResponse =
                new PersistentSearchResponse(request.getAsyncSearchId(),
                    shardSearchRequest.shardId(),
                    searchResponse);
            searchStorageService.storeResult(persistentSearchResponse, storeListener);
        }, listener::onFailure);

        storeListener.whenComplete(r -> listener.onResponse(TransportResponse.Empty.INSTANCE), listener::onFailure);

        searchService.executeQueryPhase(shardSearchRequest, keepStatesInContext, task, queryListener);
    }

    public void executePartialReduce(ReducePartialResultsRequest request,
                                     SearchShardTask task,
                                     ActionListener<TransportResponse.Empty> listener) {
        runAsync(() -> runPartialReduce(request, null), listener);
    }

    static class PartialResultReducer {
        private final SearchResponseMerger responseMerger;

    }

    public TransportResponse.Empty runPartialReduce(ReducePartialResultsRequest request, ActionListener<SearchResponse> listener) {
        // we need to use versioning for SearchResponse (so we avoid conflicting operations)
        // store the number of reduced shards (this would allow to run the last reduction)
        final String searchId = request.getSearchId();
        StepListener<SearchResponse> getSearchResultListener = new StepListener<>();
        getSearchResultListener.whenComplete((searchResponse -> {
            try {
                // TODO: Extract to a class that keeps track of versioning and base search response
                final SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                    SearchService.DEFAULT_FROM,
                    SearchService.DEFAULT_SIZE,
                    SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
                    new TransportSearchAction.SearchTimeProvider(0, 0, () -> 1L),
                    searchPhaseController.getReduceContext(request.getOriginalRequest())
                );

                if (searchResponse != null) {
                    searchResponseMerger.add(searchResponse);
                }

                final SearchResponse reducedResponse = reduce(searchResponseMerger, request);
                listener.onResponse(reducedResponse);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }), listener::onFailure);
        searchStorageService.getTotalPersistentSearchResult(searchId, getSearchResultListener);

        return TransportResponse.Empty.INSTANCE;
    }

    private void loadPartialResults(ReducePartialResultsRequest request, ActionListener<Collection<PersistentSearchResponse>> listener) {
        final List<SearchShard> shardsToReduce = request.getShardsToReduce();
        GroupedActionListener<PersistentSearchResponse> groupListener = new GroupedActionListener<>(listener, shardsToReduce.size());

        for (SearchShard searchShard : shardsToReduce) {
            final String id = PersistentSearchResponse.generateId(request.getSearchId(), searchShard.getShardId());
            searchStorageService.getPersistentSearchResult(id, groupListener);
        }
    }

    private SearchResponse reduce(SearchResponseMerger searchResponseMerger, ReducePartialResultsRequest request) {

        for (SearchShard searchShard : request.getShardsToReduce()) {
            final String partialResultId = PersistentSearchResponse.generateId(request.getSearchId(), searchShard.getShardId());
            final SearchResponse partialResult
                = searchStorageService.getPartialResult(partialResultId);
            searchResponseMerger.add(partialResult);
        }

        return searchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY);
    }


    private SearchResponse convertToSearchResponse(QueryFetchSearchResult result,
                                                  InternalAggregation.ReduceContextBuilder aggReduceContextBuilder) {
        // TODO: Check this
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
