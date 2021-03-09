/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.search.persistent.PersistentSearchResponse;

import java.util.Collections;
import java.util.List;

public class PartialReducedResponse {
    private final PersistentSearchResponse searchResponse;
    private final List<PersistentSearchShardId> reducedShards;
    private final List<PersistentSearchShardFetchFailure> failedToFetchShards;

    public PartialReducedResponse(PersistentSearchResponse searchResponse,
                                  List<PersistentSearchShardId> reducedShards,
                                  List<PersistentSearchShardFetchFailure> failedShards) {
        this.searchResponse = searchResponse;
        this.reducedShards = Collections.unmodifiableList(reducedShards);
        this.failedToFetchShards = failedShards == null ? Collections.emptyList() : Collections.unmodifiableList(failedShards);
    }

    public PersistentSearchResponse getSearchResponse() {
        return searchResponse;
    }

    public List<PersistentSearchShardId> getReducedShards() {
        return reducedShards;
    }

    public List<PersistentSearchShardFetchFailure> getFailedToFetchShards() {
        return failedToFetchShards;
    }
}
