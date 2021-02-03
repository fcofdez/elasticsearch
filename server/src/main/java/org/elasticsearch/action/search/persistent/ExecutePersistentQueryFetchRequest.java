/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class ExecutePersistentQueryFetchRequest extends ActionRequest {
    private final String asyncSearchId;
    private final ShardSearchRequest shardSearchRequest;

    public ExecutePersistentQueryFetchRequest(String asyncSearchId, ShardSearchRequest shardSearchRequest) {
        this.asyncSearchId = asyncSearchId;
        this.shardSearchRequest = shardSearchRequest;
    }

    public ExecutePersistentQueryFetchRequest(StreamInput in) throws IOException {
        super(in);
        this.asyncSearchId = in.readString();
        this.shardSearchRequest = new ShardSearchRequest(in);
    }

    public String getAsyncSearchId() {
        return asyncSearchId;
    }

    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(asyncSearchId);
        shardSearchRequest.writeTo(out);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return shardSearchRequest.createTask(id, type, action, parentTaskId, headers);
    }
}
