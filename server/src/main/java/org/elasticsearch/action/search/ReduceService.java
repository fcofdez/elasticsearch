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
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.search.PersistentSearchStorageService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ReducePartialResultsRequest;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class ReduceService {
    private final PersistentSearchStorageService persistentSearchStorageService;
    private final Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder;
    private final SearchPhaseController searchPhaseController;

    public ReduceService(PersistentSearchStorageService persistentSearchStorageService,
                         Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder,
                         SearchPhaseController searchPhaseController) {
        this.persistentSearchStorageService = persistentSearchStorageService;
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;
        this.searchPhaseController = searchPhaseController;
    }

    public void execute(ReducePartialResultsRequest request, ActionListener<SearchResponse> listener) {
        final String searchId = request.getSearchId();
        final List<SearchShard> shardsToReduce = request.getShardsToReduce();
        GroupedActionListener<PersistentSearchResponse> l = new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<PersistentSearchResponse> persistentSearchResponses) {
                // Run in a different thread
                final SearchResponse reducedSearchResponse = reduce(persistentSearchResponses, request.getOriginalRequest());
                listener.onResponse(reducedSearchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, shardsToReduce.size());

        for (SearchShard searchShard : shardsToReduce) {
            final String id = PersistentSearchResponse.generateId(searchId, searchShard.getShardId());
            persistentSearchStorageService.getPersistentSearchResult(id, l);
        }
    }

    public SearchResponse convertToSearchResponse(QueryFetchSearchResult result,
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

    private SearchResponse reduce(Collection<PersistentSearchResponse> responses, SearchRequest searchRequest) {
        final SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
            SearchService.DEFAULT_FROM,
            SearchService.DEFAULT_SIZE,
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 1L),
            requestToAggReduceContextBuilder.apply(searchRequest)
        );

        for (PersistentSearchResponse response : responses) {
            final SearchResponse searchResponse = response.getSearchResponse();
            searchResponseMerger.add(searchResponse);
        }

        return searchResponseMerger.getMergedResponse(SearchResponse.Clusters.EMPTY);
    }
}
