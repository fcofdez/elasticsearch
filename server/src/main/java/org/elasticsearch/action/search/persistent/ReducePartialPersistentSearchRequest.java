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
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ReducePartialPersistentSearchRequest extends ActionRequest {
    private final String searchId;
    private final List<SearchShard> shardsToReduce;
    private final SearchRequest originalRequest;
    private final boolean executeFinalReduce;
    private final long searchAbsoluteStartMillis;
    private final long searchRelativeStartNanos;

    public ReducePartialPersistentSearchRequest(String searchId,
                                                List<SearchShard> shardsToReduce,
                                                SearchRequest originalRequest,
                                                boolean executeFinalReduce,
                                                long searchAbsoluteStartMillis,
                                                long searchRelativeStartNanos) {
        this.searchId = searchId;
        this.shardsToReduce = shardsToReduce;
        this.originalRequest = originalRequest;
        this.executeFinalReduce = executeFinalReduce;
        this.searchAbsoluteStartMillis = searchAbsoluteStartMillis;
        this.searchRelativeStartNanos = searchRelativeStartNanos;
    }

    public ReducePartialPersistentSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchId = in.readString();
        this.shardsToReduce = in.readList(SearchShard::new);
        this.originalRequest = new SearchRequest(in);
        this.executeFinalReduce = in.readBoolean();
        this.searchAbsoluteStartMillis = in.readLong();
        this.searchRelativeStartNanos = in.readLong();
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

    public long getSearchAbsoluteStartMillis() {
        return searchAbsoluteStartMillis;
    }

    public long getSearchRelativeStartNanos() {
        return searchRelativeStartNanos;
    }

    public boolean isFinalReduce() {
        return executeFinalReduce;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(searchId);
        out.writeList(shardsToReduce);
        originalRequest.writeTo(out);
        out.writeBoolean(executeFinalReduce);
        out.writeLong(searchAbsoluteStartMillis);
        out.writeLong(searchRelativeStartNanos);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::getDescription, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
