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

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

public class PersistentSearchResponse implements ToXContent {
    private final String searchId;
    private final ShardId shardId;
    private final SearchResponse searchResponse;

    public PersistentSearchResponse(String searchId,
                                    ShardId shardId,
                                    SearchResponse searchResponse) {
        this.searchId = searchId;
        this.shardId = shardId;
        this.searchResponse = searchResponse;
    }

    public String generateId() {
        return generateId(searchId, shardId);
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public static String generateId(String searchId, ShardId shardId) {
        final String index = shardId.getIndex().toString();
        return String.join("/", searchId, index, Integer.toString(shardId.getId()));
    }

    public static PersistentSearchResponse fromXContent(final Map<String, Object> source) throws Exception {
        final String searchId = (String) source.get("search_id");
        if (searchId == null) {
            throw invalidDoc("search_id");
        }
        final String indexName = (String) source.get("index_name");
        if (indexName == null) {
            throw invalidDoc("index_name");
        }
        final String indexUUID = (String) source.get("index_uuid");
        if (indexUUID == null) {
            throw invalidDoc("index_name");
        }

        final Integer shardId = (Integer) source.get("shard_id");
        if (shardId == null) {
            throw invalidDoc("shard_id");
        }
        final BytesReference queryResult = (BytesReference) source.get("query_fetch_result");
        if (queryResult == null) {
            throw invalidDoc("query_fetch_result");
        }
        SearchResponse searchResponse = decodeSearchResponse(queryResult);

        return new PersistentSearchResponse(searchId, new ShardId(new Index(indexName, indexUUID), shardId), searchResponse);
    }

    private static IllegalArgumentException invalidDoc(String missingField) {
        return new IllegalArgumentException("Invalid document, '" + missingField + "' field is missing");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("creation_time", 10L);
            builder.field("search_id", searchId);
            final Index index = shardId.getIndex();
            builder.field("index_name", index.getName());
            builder.field("index_uuid", index.getUUID());
            builder.field("shard_id", shardId.getId());
            builder.field("query_fetch_result", encodeSearchResponse(searchResponse));
        }
        return builder.endObject();
    }

    private BytesReference encodeSearchResponse(SearchResponse searchResponse) throws IOException {
        // TODO: introduce circuit breaker
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            searchResponse.writeTo(out);
            return out.bytes();
        }
    }

    private static SearchResponse decodeSearchResponse(BytesReference encodedQuerySearchResult) throws Exception {
        try (StreamInput in = encodedQuerySearchResult.streamInput()) {
            return new SearchResponse(in);
        }
    }
}
