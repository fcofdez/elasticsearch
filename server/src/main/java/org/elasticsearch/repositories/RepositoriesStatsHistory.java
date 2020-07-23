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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RepositoriesStatsHistory implements Writeable {
    private final int maxElements;
    private final Map<String, RepositoryStatsHistory> reposHistory;

    RepositoriesStatsHistory(int maxElements) {
        this(maxElements, new HashMap<>());
    }

    RepositoriesStatsHistory(int maxElements, Map<String, RepositoryStatsHistory> reposHistory) {
        this.maxElements = maxElements;
        this.reposHistory = reposHistory;
    }

    public RepositoriesStatsHistory(StreamInput in) throws IOException {
        this.maxElements = in.readInt();
        this.reposHistory = in.readMap(StreamInput::readString, RepositoryStatsHistory::new);
    }

    synchronized void cleanOldRepoStats(Duration age) {
        Iterator<Map.Entry<String, RepositoryStatsHistory>> it = reposHistory.entrySet().iterator();
        while (it.hasNext()) {
            RepositoryStatsHistory repositoryStatsHistory = it.next().getValue();

            repositoryStatsHistory.evictStatsOlderThan(age);
            if (repositoryStatsHistory.isEmpty()) {
                it.remove();
            }
        }
    }

    synchronized void addRepositoryStats(RepositoryStats repositoryStats) {
        RepositoryStatsHistory repositoryStatsHistory = reposHistory.computeIfAbsent(repositoryStats.id(),
            repoId -> new RepositoryStatsHistory(repoId, 5));
        repositoryStatsHistory.addRepositoryStats(repositoryStats);
    }

    synchronized RepositoriesStatsHistory merge(RepositoriesStatsHistory other) {
        if (maxElements != other.maxElements) {
            throw new IllegalArgumentException();
        }

        Map<String, RepositoryStatsHistory> histories = new HashMap<>(reposHistory);
        for (Map.Entry<String, RepositoryStatsHistory> currentRepoEntry : other.reposHistory.entrySet()) {
            histories.merge(currentRepoEntry.getKey(), currentRepoEntry.getValue(), RepositoryStatsHistory::merge);
        }

        return new RepositoriesStatsHistory(maxElements, histories);
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        out.writeInt(maxElements);
        out.writeMap(reposHistory, StreamOutput::writeString, (o, stats) -> stats.writeTo(o));
    }
}
