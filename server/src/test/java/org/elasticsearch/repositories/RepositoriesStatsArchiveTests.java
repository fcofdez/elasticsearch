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


import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.Collections;

public class RepositoriesStatsArchiveTests extends ESTestCase {
    public void testMaxSizeIsEnforced() throws Exception {
        int maxElements = randomIntBetween(0, 5);
        RepositoriesStatsArchive repositoriesStatsArchive = new RepositoriesStatsArchive(5);
        for (int i = 0; i < maxElements + 1; i++) {
            RepositoryStats stats = new RepositoryStats("name", "type", "location", Collections.emptyMap(), Instant.now());
            repositoriesStatsArchive.addRepositoryStats(stats);
        }
    }

}
