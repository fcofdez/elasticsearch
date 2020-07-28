/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.repositories.stats.s3;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.repositories.stats.AbstractRepositoriesStatsAPIRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.*;

public class S3RepositoriesStatsIT extends AbstractRepositoriesStatsAPIRestTestCase {

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected String repositoryLocation() {
        // TODO fix this
        return System.getProperty("test.s3.bucket") + "/" + System.getProperty("test.s3.base_path");
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        Settings settings = repositorySettings();
        return Settings.builder().put(settings).put("s3.client.max_retries",4).build();
    }

    @Override
    protected void assertRequestCountersAccountedForReadValues(Map<String, Integer> requestCounters) {
        assertThat(requestCounters.get("GET"), is(greaterThan(0)));
        assertThat(requestCounters.get("LIST"), is(greaterThan(0)));
    }

    @Override
    protected void assertRequestCountersAccountedForWriteValues(Map<String, Integer> requestCounters) {
        assertThat(requestCounters.get("PUT"), is(greaterThan(0)));
    }
}
