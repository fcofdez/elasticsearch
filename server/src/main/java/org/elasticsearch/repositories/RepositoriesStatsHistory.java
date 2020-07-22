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

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RepositoriesStatsHistory extends AbstractLifecycleComponent implements Writeable {
    private final Map<String, RepositoryStatsHistory> reposHistory;

    private final ThreadPool threadPool;
    private Scheduler.Cancellable cleaningTask;
    // TODO Cleaning interval
    // TODO max age
    // TODO max element count per repository

    public RepositoriesStatsHistory(ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.reposHistory = new HashMap<>();
    }

    public RepositoriesStatsHistory(StreamInput in) throws IOException {
        this.threadPool = null;
        this.reposHistory = in.readMap(StreamInput::readString, RepositoryStatsHistory::new);
    }

    private synchronized void cleanOldRepoStats() {
        Iterator<Map.Entry<String, RepositoryStatsHistory>> it = reposHistory.entrySet().iterator();
        while (it.hasNext()) {
            RepositoryStatsHistory repositoryStatsHistory = it.next().getValue();

            repositoryStatsHistory.evictStatsOlderThan(Duration.ZERO);
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

    synchronized Map<String, RepositoryStatsHistory> merge(Map<String, RepositoryStatsHistory> currentRepos) {
        // TODO convert that argument as a RepositoriesStatsHistory? and schedule outside?
        Map<String, RepositoryStatsHistory> histories = new HashMap<>(reposHistory);
        for (Map.Entry<String, RepositoryStatsHistory> currentRepoEntry : currentRepos.entrySet()) {
            histories.merge(currentRepoEntry.getKey(), currentRepoEntry.getValue(), RepositoryStatsHistory::merge);
        }
        return histories;
    }

    @Override
    protected synchronized void doStart() {
        cleaningTask = threadPool.scheduleWithFixedDelay(this::cleanOldRepoStats, TimeValue.ZERO, ThreadPool.Names.GENERIC);
    }

    @Override
    protected synchronized void doStop() {
        if (cleaningTask != null) {
            cleaningTask.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        out.writeMap(reposHistory, StreamOutput::writeString, (o, stats) -> stats.writeTo(o));
    }
}
