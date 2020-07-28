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
    private final RepositoryId repositoryId;
    private final int maxElements;
    private final TimeValue retention;

    private final Deque<RepositoryStatsSnapshot> archive = new ArrayDeque<>();

    RepositoryStatsArchive(RepositoryId repositoryId, int maxElements, TimeValue retentionPeriod) {
        if (maxElements < 0) {
            throw new IllegalArgumentException("Invalid");
        }

        this.repositoryId = repositoryId;
        this.maxElements = maxElements;
        this.retention = retentionPeriod;
    }

    void addRepositoryStats(RepositoryStatsSnapshot repositoryStats) {
        assert repositoryStats.getId().equals(repositoryId);

        archive.add(repositoryStats);
        evict();
    }

    boolean isEmpty() {
        return archive.isEmpty();
    }

    RepositoryId getRepositoryId() {
        return repositoryId;
    }

    /**
     * Returns a list of RepositoryStats in FIFO order
     * @return a list of RepositoryStats in FIFO order.
     */
    List<RepositoryStatsSnapshot> getArchivedStats() {
        return new ArrayList<>(archive);
    }

    void evict() {
        Instant retentionDeadline = getRetentionDeadline();
        RepositoryStatsSnapshot stats;
        while ((stats = archive.peek()) != null && shouldEvict(stats, retentionDeadline)) {
            archive.pollFirst();
        }
    }

    private boolean shouldEvict(RepositoryStatsSnapshot stats, Instant deadline) {
        return archive.size() > maxElements && stats.createdBefore(deadline);
    }

    private Instant getRetentionDeadline() {
        return Instant.now().minus(Duration.ofMillis(retention.getMillis()));
    }
}
