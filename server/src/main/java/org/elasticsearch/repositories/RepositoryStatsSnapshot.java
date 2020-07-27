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

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RepositoryStatsSnapshot implements Writeable, ToXContent {

    public static final RepositoryStatsSnapshot EMPTY_STATS = new RepositoryStatsSnapshot(null, Collections.emptyMap(), Instant.now());

    private final RepositoryId repositoryId;
    public final Map<String, Long> requestCounts;
    public final Instant createdAt;

    public RepositoryStatsSnapshot(RepositoryId repositoryId,
                                   Map<String, Long> requestCounts,
                                   Instant createdAt) {
        this.repositoryId = repositoryId;
        this.requestCounts = requestCounts;
        this.createdAt = createdAt;
    }

    public RepositoryStatsSnapshot(StreamInput in) throws IOException {
        this.repositoryId = new RepositoryId(in);
        this.requestCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.createdAt = in.readInstant();
    }

    public RepositoryId getId() {
        return repositoryId;
    }

    public boolean createdBefore(Instant instant) {
        return createdAt.isBefore(instant);
    }

    public RepositoryStatsSnapshot merge(RepositoryStatsSnapshot otherStats) {
        if (repositoryId.equals(otherStats.repositoryId) == false) {
            throw new IllegalArgumentException();
        }
        HashMap<String, Long> mergedRequestCounts = new HashMap<>(requestCounts);
        otherStats.requestCounts.forEach((k,v) -> mergedRequestCounts.merge(k, v, Long::sum));
        return new RepositoryStatsSnapshot(repositoryId, mergedRequestCounts, Instant.now());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositoryId.writeTo(out);
        out.writeMap(requestCounts, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeInstant(createdAt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", UUID.randomUUID().toString());
        repositoryId.toXContent(builder, params);
        builder.field("created_at", createdAt.toString());
        builder.field("request_counts", requestCounts);
        builder.endObject();
        return builder;
    }
}
