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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public final class RepositoryStatsHistory implements Writeable {
    private final String repositoryId;
    private final int maxElements;
    private final Deque<RepositoryStats> reposHistory;

    public static RepositoryStatsHistory singleton(RepositoryStats repositoryStats, int maxElements) {
        RepositoryStatsHistory repoHistory = new RepositoryStatsHistory(repositoryStats.id(), maxElements);
        repoHistory.addRepositoryStats(repositoryStats);
        return repoHistory;
    }

    RepositoryStatsHistory(String repositoryId, int maxElements) {
        this.repositoryId = repositoryId;
        this.reposHistory = new ArrayDeque<>(maxElements);
        this.maxElements = maxElements;
    }

    RepositoryStatsHistory(StreamInput in) throws IOException {
        this.repositoryId = in.readString();
        this.maxElements = in.readInt();
        this.reposHistory = new ArrayDeque<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            reposHistory.offer(new RepositoryStats(in));
        }
    }

    public List<RepositoryStats> asList() {
        List<RepositoryStats> repoHists = new ArrayList<>(reposHistory.size());
        Iterator<RepositoryStats> it = reposHistory.descendingIterator();

        while (it.hasNext()) {
            repoHists.add(it.next());
        }
        return Collections.unmodifiableList(repoHists);
    }

    boolean isEmpty() {
        return reposHistory.isEmpty();
    }

    void addRepositoryStats(RepositoryStats repositoryStats) {
        assert repositoryStats.id().equals(repositoryId);

        if (reposHistory.size() == maxElements) {
            reposHistory.poll();
        }
        reposHistory.offer(repositoryStats);
    }

    void evictStatsOlderThan(Duration duration) {
        RepositoryStats stats;
        while ((stats = reposHistory.peek()) != null && stats.olderThan(duration)) {
            reposHistory.poll();
        }
    }

    static RepositoryStatsHistory merge(RepositoryStatsHistory repositoryStatsHistory,
                                        RepositoryStatsHistory repositoryStatsHistory1) {
        return repositoryStatsHistory;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repositoryId);
        out.writeInt(maxElements);
        out.writeInt(reposHistory.size());
        for (RepositoryStats stats : reposHistory) {
            stats.writeTo(out);
        }
    }
}
