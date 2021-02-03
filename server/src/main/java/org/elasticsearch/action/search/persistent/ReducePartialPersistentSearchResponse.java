/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ReducePartialPersistentSearchResponse extends ActionResponse {
    private final List<SearchShard> reducedShards;

    public ReducePartialPersistentSearchResponse(List<SearchShard> reducedShards) {
        this.reducedShards = reducedShards;
    }

    public ReducePartialPersistentSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.reducedShards = in.readList(SearchShard::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(reducedShards);
    }
}
