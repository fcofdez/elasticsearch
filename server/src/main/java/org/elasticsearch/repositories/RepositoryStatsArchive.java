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

import org.elasticsearch.common.unit.TimeValue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public final class RepositoryStatsArchive {
    private final String repositoryId;
    private final int minElements;
    private final TimeValue retention;

    private final Deque<RepositoryStats> archive = new ArrayDeque<>();

    RepositoryStatsArchive(String repositoryId, int minElements, TimeValue retention) {
        if (minElements < 0) {
            throw new IllegalArgumentException("Invalid");
        }

        this.repositoryId = repositoryId;
        this.minElements = minElements;
        this.retention = retention;
    }

    void addRepositoryStats(RepositoryStats repositoryStats) {
        assert repositoryStats.getId().equals(repositoryId);

        archive.add(repositoryStats);
        evict();

        assert archive.size() <= minElements;
    }

    boolean isEmpty() {
        return archive.isEmpty();
    }

    int size() {
        return archive.size();
    }

    String getRepositoryId() {
        return repositoryId;
    }

    /**
     * Returns a list of RepositoryStats in FIFO order
     * @return a list of RepositoryStats in FIFO order.
     */
    List<RepositoryStats> getArchivedStats() {
        return new ArrayList<>(archive);
    }

    private void evict() {
        Instant retentionDeadline = getRetentionDeadline();
        RepositoryStats stats;
        // TODO Keep at least N within the time range
        while ((stats = archive.peek()) != null && shouldEvict(stats, retentionDeadline)) {
            archive.pollFirst();
        }
    }

    private boolean shouldEvict(RepositoryStats stats, Instant deadline) {
        return archive.size() > minElements || stats.stoppedBefore(deadline);
    }

    private Instant getRetentionDeadline() {
        return Instant.now().minus(Duration.ofMillis(retention.getMillis()));
    }
}
