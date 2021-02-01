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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.persistent.PersistentSearchId;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;

public class SubmitPersistentSearchResponse extends ActionResponse implements StatusToXContentObject {
    private PersistentSearchId searchId;

    public SubmitPersistentSearchResponse(PersistentSearchId searchId) {
        this.searchId = searchId;
    }

    public SubmitPersistentSearchResponse(StreamInput in) throws IOException {
        super(in);
        this.searchId = PersistentSearchId.decode(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(searchId.getEncodedId());
    }

    public PersistentSearchId getSearchId() {
        return searchId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.ACCEPTED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", searchId.getEncodedId());
        builder.endObject();
        return builder;
    }

    public static SubmitPersistentSearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        if (fieldName.equals("id") == false) {
            throwUnknownField(fieldName, parser.getTokenLocation());
        }
        parser.nextToken();
        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.currentToken(), parser);
        String id = parser.text();
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new SubmitPersistentSearchResponse(PersistentSearchId.decode(id));
    }
}
