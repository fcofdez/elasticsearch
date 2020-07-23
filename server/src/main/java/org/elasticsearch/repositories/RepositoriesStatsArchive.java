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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class RepositoriesStatsArchive {
    private final int maxElements;
    private final TimeValue retention;
    private final Map<String, RepositoryStatsArchive> archives;

    RepositoriesStatsArchive(int maxElements, TimeValue retention) {
        if (maxElements < 0) {
            throw new IllegalArgumentException("maxElements must be greater than or equal to 0");
        }
        this.maxElements = maxElements;
        this.retention = retention;
        this.archives = new HashMap<>();
    }

    synchronized void addRepositoryStats(RepositoryStats repositoryStats) {
        RepositoryStatsArchive repositoryStatsArchive = archives.computeIfAbsent(repositoryStats.getId(),
            repoId -> new RepositoryStatsArchive(repoId, maxElements, retention));
        repositoryStatsArchive.addRepositoryStats(repositoryStats);
        cleanEmptyRepositoryArchives();
    }

    synchronized void cleanEmptyRepositoryArchives() {
        archives.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }

    synchronized List<RepositoryStats> merge(List<RepositoryStats> activeRepositoryStats) {
        cleanEmptyRepositoryArchives();

        Map<String, List<RepositoryStats>> repositoryStatsById = new HashMap<>();
        for (RepositoryStatsArchive archive : archives.values()) {
            repositoryStatsById.put(archive.getRepositoryId(), archive.getArchivedStats());
        }

        for (RepositoryStats activeRepositoryStat : activeRepositoryStats) {
            List<RepositoryStats> repositoryStats = repositoryStatsById.computeIfAbsent(activeRepositoryStat.getId(), k -> new ArrayList<>());
            repositoryStats.addAll(activeRepositoryStats);
        }

        return repositoryStatsById.values().stream().flatMap(Collection::stream).collect(Collectors.toUnmodifiableList());
    }
}
