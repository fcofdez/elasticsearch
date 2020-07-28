/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.repositories.stats.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.stats.AbstractRepositoriesStatsAPIRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.*;

public class GCSRepositoriesStatsIT extends AbstractRepositoriesStatsAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected String repositoryLocation() {
        // TODO fix this
        return System.getProperty("test.gcs.bucket") + "/" + System.getProperty("test.gcs.base_path");
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.gcs.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.gcs.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "repositories_stats").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings())
            .put("gcs.client.repositories_stats.application_name", "updated")
            .build();
    }

    @Override
    protected void assertRequestCountersAccountedForReadValues(Map<String, Integer> repoCounters) {
        assertThat(repoCounters.get("GET"), is(greaterThan(0)));
        assertThat(repoCounters.get("LIST"), is(greaterThan(0)));
    }

    @Override
    protected void assertRequestCountersAccountedForWriteValues(Map<String, Integer> repoCounters) {
        assertThat(repoCounters.get("POST"), is(greaterThan(0)));
    }
}
