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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class AsyncShardSearchRequest extends TransportRequest {
    private final String asyncSearchId;
    private final ShardSearchRequest shardSearchRequest;

    public AsyncShardSearchRequest(String asyncSearchId, ShardSearchRequest shardSearchRequest) {
        this.asyncSearchId = asyncSearchId;
        this.shardSearchRequest = shardSearchRequest;
    }

    public AsyncShardSearchRequest(StreamInput in) throws IOException {
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
}
