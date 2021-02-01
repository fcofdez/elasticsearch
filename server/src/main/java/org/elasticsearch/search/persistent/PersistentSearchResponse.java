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

package org.elasticsearch.search.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.persistent.PersistentSearchStorageService.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.ID_FIELD;
import static org.elasticsearch.search.persistent.PersistentSearchStorageService.RESPONSE_FIELD;

public class PersistentSearchResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final SearchResponse searchResponse;

    public PersistentSearchResponse(String id,
                                    SearchResponse searchResponse) {
        this.id = id;
        this.searchResponse = searchResponse;
    }

    public PersistentSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.searchResponse = new SearchResponse(in);
    }

    public String getId() {
        return id;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public static String generatePartialResultIdId(String searchId, ShardId shardId) {
        final String index = shardId.getIndex().toString();
        return String.join("/", searchId, index, Integer.toString(shardId.getId()));
    }

    public static PersistentSearchResponse fromXContent(final Map<String, Object> source) throws Exception {
        final String searchId = (String) source.get(ID_FIELD);
        if (searchId == null) {
            throw invalidDoc(ID_FIELD);
        }

        final BytesReference queryResult = (BytesReference) source.get(RESPONSE_FIELD);
        if (queryResult == null) {
            throw invalidDoc(RESPONSE_FIELD);
        }
        SearchResponse searchResponse = decodeSearchResponse(queryResult);

        return new PersistentSearchResponse(searchId, searchResponse);
    }

    private static IllegalArgumentException invalidDoc(String missingField) {
        return new IllegalArgumentException("Invalid document, '" + missingField + "' field is missing");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        searchResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD, id);
            builder.field(RESPONSE_FIELD, encodeSearchResponse(searchResponse));
            //builder.field(EXPIRATION_TIME_FIELD, System.currentTimeMillis());
            // TODO: Store queried shards?
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
