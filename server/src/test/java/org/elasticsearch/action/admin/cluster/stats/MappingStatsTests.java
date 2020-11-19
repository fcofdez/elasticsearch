/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MappingStatsTests extends AbstractWireSerializingTestCase<MappingStats> {

    @Override
    protected Reader<MappingStats> instanceReader() {
        return MappingStats::new;
    }

    @Override
    protected MappingStats createTestInstance() {
        Collection<IndexFeatureStats> stats = new ArrayList<>();
        if (randomBoolean()) {
            IndexFeatureStats s = new IndexFeatureStats("keyword");
            s.count = 10;
            s.indexCount = 7;
            stats.add(s);
        }
        if (randomBoolean()) {
            IndexFeatureStats s = new IndexFeatureStats("integer");
            s.count = 3;
            s.indexCount = 3;
            stats.add(s);
        }
        return new MappingStats(stats);
    }

    @Override
    protected MappingStats mutateInstance(MappingStats instance) throws IOException {
        List<IndexFeatureStats> fieldTypes = new ArrayList<>(instance.getFieldTypeStats());
        boolean remove = fieldTypes.size() > 0 && randomBoolean();
        if (remove) {
            fieldTypes.remove(randomInt(fieldTypes.size() - 1));
        }
        if (remove == false || randomBoolean()) {
            IndexFeatureStats s = new IndexFeatureStats("float");
            s.count = 13;
            s.indexCount = 2;
            fieldTypes.add(s);
        }
        return new MappingStats(fieldTypes);
    }

    public void testAccountsRegularIndices() {
        String mapping = "{\"properties\":{\"bar\":{\"type\":\"long\"}}}";
        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo")
                .settings(settings)
                .putMapping(mapping);
        Metadata metadata = new Metadata.Builder()
                .put(indexMetadata)
                .build();
        MappingStats mappingStats = MappingStats.of(metadata);
        IndexFeatureStats expectedStats = new IndexFeatureStats("long");
        expectedStats.count = 1;
        expectedStats.indexCount = 1;
        assertEquals(
                Collections.singleton(expectedStats),
                mappingStats.getFieldTypeStats());
    }

    public void testIgnoreSystemIndices() {
        String mapping = "{\"properties\":{\"bar\":{\"type\":\"long\"}}}";
        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        IndexMetadata.Builder indexMetadata = new IndexMetadata.Builder("foo")
                .settings(settings)
                .putMapping(mapping)
                .system(true);
        Metadata metadata = new Metadata.Builder()
                .put(indexMetadata)
                .build();
        MappingStats mappingStats = MappingStats.of(metadata);
        assertEquals(Collections.emptySet(), mappingStats.getFieldTypeStats());
    }
}
