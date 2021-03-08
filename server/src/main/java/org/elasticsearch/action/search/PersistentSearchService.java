/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchRequest;
import org.elasticsearch.action.search.persistent.ExecutePersistentQueryFetchResponse;
import org.elasticsearch.action.search.persistent.GetPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.PartialReducedResponse;
import org.elasticsearch.action.search.persistent.ShardQueryResultInfo;
import org.elasticsearch.action.search.persistent.PersistentSearchShardId;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.ReducePartialPersistentSearchResponse;
import org.elasticsearch.action.search.persistent.ShardQueryResultFetcher;
import org.elasticsearch.action.search.persistent.ShardSearchResult;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.persistent.PersistentSearchResponse;
import org.elasticsearch.search.persistent.PersistentSearchStorageService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PersistentSearchService {
    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);

    private final SearchService searchService;
    private final SearchPhaseController searchPhaseController;
    private final PersistentSearchStorageService searchStorageService;
    private final ThreadPool threadPool;
    private final LongSupplier relativeCurrentNanosProvider;
    private final Supplier<String> localNodeIdSupplier;
    private final ShardQueryResultFetcher shardQueryResultFetcher;
    private final CircuitBreaker circuitBreaker;

    public PersistentSearchService(SearchService searchService,
                                   SearchPhaseController searchPhaseController,
                                   PersistentSearchStorageService searchStorageService,
                                   ThreadPool threadPool,
                                   LongSupplier relativeCurrentNanosProvider,
                                   Supplier<String> localNodeIdSupplier,
                                   ShardQueryResultFetcher shardQueryResultFetcher,
                                   CircuitBreaker circuitBreaker) {
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
        this.searchStorageService = searchStorageService;
        this.threadPool = threadPool;
        this.relativeCurrentNanosProvider = relativeCurrentNanosProvider;
        this.localNodeIdSupplier = localNodeIdSupplier;
        this.shardQueryResultFetcher = shardQueryResultFetcher;
        this.circuitBreaker = circuitBreaker;
    }

    public void getPersistentSearchResponse(GetPersistentSearchRequest getPersistentSearchRequest,
                                            ActionListener<PersistentSearchResponse> listener) {
        searchStorageService.getPersistentSearchResponseAsync(getPersistentSearchRequest.getId(),
            ActionListener.delegateResponse(listener, (delegate, e) -> {
                if (e instanceof IndexNotFoundException) {
                    delegate.onResponse(null);
                    return;
                }
                delegate.onFailure(e);
            }));
    }

    public void executeAsyncQueryPhase(ExecutePersistentQueryFetchRequest request,
                                       SearchShardTask task, ActionListener<ExecutePersistentQueryFetchResponse> listener) {
        StepListener<SearchPhaseResult> queryListener = new StepListener<>();
        StepListener<String> storeListener = new StepListener<>();

        final ShardSearchRequest shardSearchRequest = request.getShardSearchRequest();
        queryListener.whenComplete(result -> {

            String docId = request.getResultDocId();
            String searchId = request.getSearchId();
            int shardIndex = request.getShardIndex();
            long expireTime = request.getExpireTime();

            final ShardSearchResult shardSearchResult =
                new ShardSearchResult(docId, searchId, shardIndex, expireTime, (QueryFetchSearchResult) result);

            searchStorageService.storeShardResult(shardSearchResult, storeListener);
        }, listener::onFailure);

        storeListener.whenComplete(partialResultDocId ->
                listener.onResponse(new ExecutePersistentQueryFetchResponse(partialResultDocId, localNodeIdSupplier.get())),
            listener::onFailure);

        searchService.executeQueryAndFetch(shardSearchRequest, false, task, queryListener);
    }

    public void executePartialReduce(ReducePartialPersistentSearchRequest request,
                                     SearchTask task,
                                     ActionListener<ReducePartialPersistentSearchResponse> listener) {
        // we need to use versioning for SearchResponse (so we avoid conflicting operations)
        final String searchId = request.getSearchId();
        StepListener<PersistentSearchResponse> getSearchResultListener = new StepListener<>();
        StepListener<PartialReducedResponse> reduceListener = new StepListener<>();

        getSearchResultListener.whenComplete(persistentSearchResponse -> {
            try {
                // TODO: Account for memory consumed by base partial response
                final SearchRequest originalRequest = request.getOriginalRequest();
                // TODO: This doesn't work if the reduce phase is executed in a different node than the coordinator
                final TransportSearchAction.SearchTimeProvider searchTimeProvider =
                    new TransportSearchAction.SearchTimeProvider(request.getSearchAbsoluteStartMillis(),
                        request.getSearchRelativeStartNanos(), relativeCurrentNanosProvider);
                PersistentSearchResponseMerger searchResponseMerger = new PersistentSearchResponseMerger(
                    request.getSearchId(),
                    request.getExpirationTime(),
                    originalRequest,
                    searchTimeProvider,
                    threadPool,
                    circuitBreaker,
                    searchPhaseController.getReduceContext(originalRequest),
                    persistentSearchResponse,
                    searchPhaseController,
                    request.getShardsToReduce().stream().map(ShardQueryResultInfo::getShardId).collect(Collectors.toList())
                );

                runAsync(() -> reduce(searchResponseMerger, task, request), reduceListener);
            } catch (Exception e) {
                logger.info("Error reducing!!", e);
                listener.onFailure(e);
            }
        }, listener::onFailure);

        reduceListener.whenComplete((partialReducedResponse -> {
            final PersistentSearchResponse reducedSearchResponse = partialReducedResponse.getSearchResponse();
            final ReducePartialPersistentSearchResponse reducePartialPersistentSearchResponse =
                new ReducePartialPersistentSearchResponse(partialReducedResponse.getReducedShards(),
                    partialReducedResponse.getFailedToFetchShards());

            // TODO: Add timeouts
            searchStorageService.storeResult(reducedSearchResponse, new ActionListener<>() {
                @Override
                public void onResponse(String docId) {
                    listener.onResponse(reducePartialPersistentSearchResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }), listener::onFailure);

        searchStorageService.getPersistentSearchResponseAsync(searchId,
            ActionListener.delegateResponse(getSearchResultListener, (delegate, e) -> {
                if (e instanceof IndexNotFoundException) {
                    delegate.onResponse(null);
                    return;
                }
                delegate.onFailure(e);
            })
        );
    }

    private PartialReducedResponse reduce(PersistentSearchResponseMerger searchResponseMerger,
                                          SearchTask searchTask,
                                          ReducePartialPersistentSearchRequest request) throws Exception {
        for (ShardQueryResultInfo shardQueryResultInfo : request.getShardsToReduce()) {
            checkForCancellation(searchTask);

            final PersistentSearchShardId shardId = shardQueryResultInfo.getShardId();
            // TODO: Extract as a parameter
            final TimeValue timeout = TimeValue.timeValueSeconds(1);

            try {
                // Since we want to bound the amount of used memory to the number of search threads, we fetch the results blocking
                final ShardSearchResult searchShardResult = shardQueryResultFetcher.getSearchShardResultBlocking(shardId.getDocId(),
                    shardQueryResultInfo.getNodeId(),
                    searchTask,
                    timeout
                );
                logger.info("IZUU Fetch result {}", searchShardResult.getPersistentSearchId());
                searchResponseMerger.addResponse(shardId, searchShardResult);
            } catch (Exception e) {
                logger.info("IZUU Fetch result ERROR" + shardId, e);
                searchResponseMerger.onShardResponseFetchFailure(shardQueryResultInfo, e);
            }
        }

        checkForCancellation(searchTask);
        return searchResponseMerger.getMergedResponse();
    }

    private void checkForCancellation(SearchTask searchTask) {
        if (searchTask.isCancelled()) {
            throw new RuntimeException("Search has been cancelled");
        }
    }

    private <T> void runAsync(CheckedSupplier<T, Exception> executable, ActionListener<T> listener) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(ActionRunnable.supply(listener, executable));
    }
}
