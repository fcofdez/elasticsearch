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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RepositoriesStatsArchive {
    private final int maxStatsPerRepository;
    private final TimeValue retentionPeriod;
    private final Map<RepositoryId, RepositoryStatsArchive> archives;

    RepositoriesStatsArchive(int maxStatsPerRepository, TimeValue retentionPeriod) {
        if (maxStatsPerRepository < 0) {
            throw new IllegalArgumentException("minElements must be greater than or equal to 0");
        }
        this.maxStatsPerRepository = maxStatsPerRepository;
        this.retentionPeriod = retentionPeriod;
        this.archives = new HashMap<>();
    }

    synchronized void addRepositoryStats(RepositoryStatsSnapshot repositoryStats) {
        RepositoryStatsArchive repositoryStatsArchive = archives.computeIfAbsent(repositoryStats.getId(),
            repoId -> new RepositoryStatsArchive(repoId, maxStatsPerRepository, retentionPeriod));
        repositoryStatsArchive.addRepositoryStats(repositoryStats);
        cleanEmptyRepositoryArchives();
    }

    synchronized Map<RepositoryId, List<RepositoryStatsSnapshot>> asMap() {
        cleanEmptyRepositoryArchives();

        Map<RepositoryId, List<RepositoryStatsSnapshot>> repositoryStatsById = new HashMap<>();
        for (RepositoryStatsArchive archive : archives.values()) {
            repositoryStatsById.put(archive.getRepositoryId(), archive.getArchivedStats());
        }

        return Collections.unmodifiableMap(repositoryStatsById);
    }

    synchronized void cleanEmptyRepositoryArchives() {
        archives.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }
}
