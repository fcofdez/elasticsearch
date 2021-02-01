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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ReducePartialPersistentSearchRequest extends ActionRequest {
    private final String searchId;
    private final List<SearchShard> shardsToReduce;
    private final SearchRequest originalRequest;
    private final boolean executeFinalReduce;

    public ReducePartialPersistentSearchRequest(String searchId, List<SearchShard> shardsToReduce, SearchRequest originalRequest) {
        this.searchId = searchId;
        this.shardsToReduce = shardsToReduce;
        this.originalRequest = originalRequest;
        this.executeFinalReduce = false;
    }

    public ReducePartialPersistentSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchId = in.readString();
        this.shardsToReduce = in.readList(SearchShard::new);
        this.originalRequest = new SearchRequest(in);
        this.executeFinalReduce = in.readBoolean();
    }

    public String getSearchId() {
        return searchId;
    }

    public List<SearchShard> getShardsToReduce() {
        return shardsToReduce;
    }

    public SearchRequest getOriginalRequest() {
        return originalRequest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(searchId);
        out.writeList(shardsToReduce);
        originalRequest.writeTo(out);
        out.writeBoolean(executeFinalReduce);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
