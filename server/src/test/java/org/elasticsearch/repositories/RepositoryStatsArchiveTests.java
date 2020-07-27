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
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class RepositoryStatsArchiveTests extends ESTestCase {

    public void testStatsAreStoredUntilTheyAreOlderThanTheRetentionPeriod() {
        RepositoryId repositoryId =
            new RepositoryId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));

        int retentionTimeInHours = randomIntBetween(1, 4);
        RepositoryStatsArchive repositoryStatsArchive =
            new RepositoryStatsArchive(repositoryId, 0, TimeValue.timeValueHours(retentionTimeInHours));

        int statsCount = randomInt(10);
        for (int i = 0; i < statsCount; i++) {
            RepositoryStatsSnapshot repoStats = new RepositoryStatsSnapshot(repositoryId, Collections.emptyMap(), Instant.now());
            repositoryStatsArchive.addRepositoryStats(repoStats);
        }

        assertThat(repositoryStatsArchive.getArchivedStats().size(), equalTo(statsCount));
    }

    public void testOldStatsAreRemovedOnceThereAreMoreThanMaxElementsInTheArchiveAndTheRetentionPeriodIsOver() {
        RepositoryId repositoryId =
            new RepositoryId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));

        int retentionTimeInMinutes = randomIntBetween(1, 4);
        int maxElements = randomInt(15);
        RepositoryStatsArchive repositoryStatsArchive =
            new RepositoryStatsArchive(repositoryId, maxElements, TimeValue.timeValueMinutes(retentionTimeInMinutes));

        for (int i = 0; i < maxElements + 1; i++) {
            RepositoryStatsSnapshot repoStats = new RepositoryStatsSnapshot(repositoryId,
                Collections.emptyMap(),
                Instant.now().minus(Duration.ofMinutes(retentionTimeInMinutes + 1)));
            repositoryStatsArchive.addRepositoryStats(repoStats);
        }

        assertThat(repositoryStatsArchive.getArchivedStats().size(), equalTo(maxElements));
    }
}
