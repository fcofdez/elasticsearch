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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class RepositoryStats implements Writeable {

    public static final RepositoryStats EMPTY_STATS = new RepositoryStats("", "", "", Collections.emptyMap(), null, null);

    private final String name;
    private final String type;
    private final String location;
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

    public String id() {
        return name + type + location;
    }

    public RepositoryStats stop() {
        return new RepositoryStats(name, type, location, requestCounts, startedAt, Instant.now());
    }

    public boolean olderThan(Duration age) {
        if (stoppedAt == null) {
            return false;
        }

        return Duration.between(startedAt, stoppedAt).compareTo(age) >= 0;
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
}
