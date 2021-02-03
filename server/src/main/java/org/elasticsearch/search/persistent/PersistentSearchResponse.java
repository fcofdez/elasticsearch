/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

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

    public static PersistentSearchResponse fromXContent(final Map<String, Object> source,
                                                        NamedWriteableRegistry namedWriteableRegistry) throws Exception {
        final String searchId = (String) source.get(ID_FIELD);
        if (searchId == null) {
            throw invalidDoc(ID_FIELD);
        }

        final String encodedSearchResponse = (String) source.get(RESPONSE_FIELD);
        if (encodedSearchResponse == null) {
            throw invalidDoc(RESPONSE_FIELD);
        }
        final byte[] jsonSearchResponse = Base64.getDecoder().decode(encodedSearchResponse);
        final BytesReference encodedQuerySearchResult = BytesReference.fromByteBuffer(ByteBuffer.wrap(jsonSearchResponse));
        SearchResponse searchResponse = decodeSearchResponse(encodedQuerySearchResult, namedWriteableRegistry);

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
            searchResponse.writeTo(out);
            return out.bytes();
        }
    }

    private static SearchResponse decodeSearchResponse(BytesReference encodedQuerySearchResult,
                                                       NamedWriteableRegistry namedWriteableRegistry) throws Exception {
        try (StreamInput in = new NamedWriteableAwareStreamInput(encodedQuerySearchResult.streamInput(), namedWriteableRegistry)) {
            return new SearchResponse(in);
        }
    }
}
