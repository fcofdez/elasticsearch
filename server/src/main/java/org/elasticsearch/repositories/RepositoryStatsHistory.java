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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RepositoryStatsHistory {
    private final String repositoryName;
    private final CircularBuffer reposHistory;

    public RepositoryStatsHistory(String repositoryName, int maxElements) {
        this.repositoryName = repositoryName;
        this.reposHistory = new CircularBuffer(maxElements);
    }

    void addRepositoryStats(RepositoryStats repositoryStats) {
        closeCurrentRepoIfPresent();
        reposHistory.add(new RepoHist(repositoryStats));
    }

    void closeCurrentRepoIfPresent() {
        RepoHist activeRepo = reposHistory.getCurrent();
        if (activeRepo != null) {
            activeRepo.stop();
        }
    }

    private static class CircularBuffer {
        private final int maxElements;
        private final RepoHist[] reposHistory;
        private int head;
        private int activeRepoIndex;
        private int size;

        private CircularBuffer(int maxElements) {
            this.maxElements = maxElements;
            this.reposHistory = new RepoHist[maxElements];
            this.head = 0;
            this.activeRepoIndex = 0;
            this.size = 0;
        }

        private void add(RepoHist repoHist) {
            activeRepoIndex = head;
            reposHistory[activeRepoIndex] = repoHist;
            head = (head + 1) % maxElements;

            size = Math.min(maxElements, size + 1);
        }

        List<RepoHist> lifoList() {
            List<RepoHist> repos = new ArrayList<>(size);
            for (int dstIdx = 0, srcIdx = activeRepoIndex; dstIdx < size; dstIdx++) {
                repos.add(reposHistory[srcIdx--]);
                if (srcIdx < 0) {
                    srcIdx = size;
                }
            }
            return repos;
        }

        RepoHist getCurrent() {
            return reposHistory[activeRepoIndex];
        }
    }

    static class RepoHist {
        private final UUID id;
        private final ZonedDateTime startedAt;
        private final RepositoryStats repositoryStats;
        private ZonedDateTime stoppedAt;

        public RepoHist(RepositoryStats repositoryStats) {
            this.id = UUID.randomUUID();
            this.startedAt = ZonedDateTime.now(ZoneOffset.UTC);
            this.repositoryStats = repositoryStats;
        }

        void stop() {
            assert stoppedAt == null;
            stoppedAt = ZonedDateTime.now(ZoneOffset.UTC);
        }
    }
}
