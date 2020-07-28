/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.repositories.stats.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.stats.AbstractRepositoriesStatsAPIRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.*;

public class AzureRepositoriesStatsIT extends AbstractRepositoriesStatsAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected String repositoryLocation() {
        // TODO fix this
        return System.getProperty("test.azure.container") + "/" + System.getProperty("test.azure.base_path");
    }

    @Override
    protected Settings repositorySettings() {
        final String container = System.getProperty("test.azure.container");
        assertThat(container, not(blankOrNullString()));

        final String basePath = System.getProperty("test.azure.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "repositories_stats").put("container", container).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings())
            .put("azure.client.repositories_stats.max_retries", 5)
            .build();
    }

    @Override
    protected void assertRequestCountersAccountedForReadValues(Map<String, Long> repoCounters) {
        assertThat(repoCounters.get("GET"), is(greaterThan(0L)));
        assertThat(repoCounters.get("HEAD"), is(greaterThan(0L)));
        assertThat(repoCounters.get("LIST"), is(greaterThan(0L)));
    }

    @Override
    protected void assertRequestCountersAccountedForWriteValues(Map<String, Long> repoCounters) {
        assertThat(repoCounters.get("PUT"), is(greaterThan(0L)));
    }
}
