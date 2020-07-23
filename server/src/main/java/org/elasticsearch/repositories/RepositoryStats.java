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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class RepositoryStats implements Writeable, ToXContent {

    public static final RepositoryStats EMPTY_STATS = new RepositoryStats("", "", "", Collections.emptyMap(), null, null);

    public final String name;
    public final String type;
    public final String location;
    public final Map<String, Long> requestCounts;
    public final Instant startedAt;
    @Nullable
    public final Instant stoppedAt;

    public RepositoryStats(String name,
                           String type,
                           String location,
                           Map<String, Long> requestCounts,
                           Instant startedAt) {
        this(name, type, location, requestCounts, startedAt, null);
    }

    public RepositoryStats(String name,
                           String type,
                           String location,
                           Map<String, Long> requestCounts,
                           Instant startedAt,
                           Instant stoppedAt) {
        this.name = name;
        this.type = type;
        this.location = location;
        this.requestCounts = requestCounts;
        this.startedAt = startedAt;
        this.stoppedAt = stoppedAt;
    }

    public RepositoryStats(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.location = in.readString();
        this.requestCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
        this.startedAt = in.readInstant();
        this.stoppedAt = in.readOptionalInstant();
    }

    public String getId() {
        return String.format("%s-%s-%s", name, type, location);
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public boolean stoppedBefore(Instant instant) {
        if (stoppedAt == null) {
            return false;
        }
        return stoppedAt.isBefore(instant);
    }

    public RepositoryStats stop() {
        return new RepositoryStats(name, type, location, requestCounts, startedAt, Instant.now());
    }

    public RepositoryStats merge(RepositoryStats otherStats) {
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeString(location);
        out.writeMap(requestCounts, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeInstant(startedAt);
        out.writeOptionalInstant(stoppedAt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", UUID.randomUUID().toString());
        builder.field("repository_name", name);
        builder.field("repository_type", type);
        builder.field("repository_location", location);
        builder.field("counter_started_at", startedAt.toString());
        if (stoppedAt != null) {
            builder.field("counter_stopped_at", startedAt.toString());
        }
        builder.field("request_counts", requestCounts);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryStats stats = (RepositoryStats) o;
        return Objects.equals(name, stats.name) &&
            Objects.equals(type, stats.type) &&
            Objects.equals(location, stats.location) &&
            Objects.equals(requestCounts, stats.requestCounts) &&
            Objects.equals(startedAt, stats.startedAt) &&
            Objects.equals(stoppedAt, stats.stoppedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, location, requestCounts, startedAt, stoppedAt);
    }
}
