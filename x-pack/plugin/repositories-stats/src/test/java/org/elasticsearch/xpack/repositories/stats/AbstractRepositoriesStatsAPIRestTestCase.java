/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.repositories.RepositoryId;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;

public abstract class AbstractRepositoriesStatsAPIRestTestCase extends ESRestTestCase {
    protected abstract String repositoryType();

    protected abstract String repositoryLocation();

    protected abstract Settings repositorySettings();

    /**
     * To force a new repository creation
     */
    protected abstract Settings updatedRepositorySettings();

    protected abstract void assertRequestCountersAccountedForReadValues(Map<String, Long> repoCounters);

    protected abstract void assertRequestCountersAccountedForWriteValues(Map<String, Long> repoCounters);

    @Before
    public void clear() throws Exception {
        clearRepositoriesStats();
    }

    @After
    public void clearStats() throws Exception {
        clearRepositoriesStats();
    }

    public void testStatsAreTracked() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repoStats = getRepositoriesStats();
            assertThat(repoStats.size(), equalTo(1));

            Map<String, Long> requestCounts = repoStats.get(0).requestCounts;
            assertRepositoryStatsBelongToRepository(repoStats.get(0), repository);
            assertRequestCountersAccountedForReadValues(requestCounts);
            assertRequestCountersAccountedForWriteValues(requestCounts);
        });
    }

    public void testClearRepositoriesStats() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            deleteRepository(repository);

            assertThat(getRepositoriesStats().size(), equalTo(1));

            clearRepositoriesStats();

            assertThat(getRepositoriesStats().size(), equalTo(0));
        });
    }

    public void testRegisterMultipleRepositoriesAndGetStats() throws Exception {
        List<String> repositoryNames = List.of("repo-a", "repo-b", "repo-c");
        for (String repositoryName : repositoryNames) {
            registerRepository(repositoryName, repositoryType(), false, repositorySettings());
        }

        List<RepositoryStatsSnapshot> repositoriesStats = getRepositoriesStats();
        Map<RepositoryId, List<RepositoryStatsSnapshot>> repositoriesByName
            = repositoriesStats.stream().collect(Collectors.groupingBy(r -> r.repositoryId));

        for (String repositoryName : repositoryNames) {
            List<RepositoryStatsSnapshot> repositoryStats = repositoriesByName.get(repositoryName);
            assertThat(repositoryStats, is(notNullValue()));
            assertThat(repositoryStats.size(), equalTo(1));

            RepositoryStatsSnapshot stats = repositoriesStats.get(0);
            assertRepositoryStatsBelongToRepository(stats, repositoryName);
            assertAllRequestCountsAreZero(stats);
        }
    }

    public void testStatsAreArchivedAfterRepositoryDeletion() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repoStats = getRepositoriesStats();
            assertThat(repoStats.size(), equalTo(1));
            assertRepositoryStatsBelongToRepository(repoStats.get(0), repository);

            deleteRepository(repository);

            List<RepositoryStatsSnapshot> repoStatsAfterDeletion = getRepositoriesStats();
            assertThat(repoStats, equalTo(repoStatsAfterDeletion));
        });
    }

    public void testStatsAreStoredIntoANewCounterInstanceAfterRepoConfigUpdate() throws Exception {
        final String snapshot = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        snapshotAndRestoreIndex(snapshot, (repository, index) -> {
            List<RepositoryStatsSnapshot> repositoriesStats = getRepositoriesStats();
            assertThat(repositoriesStats.size(), equalTo(1));
            assertRepositoryStatsBelongToRepository(repositoriesStats.get(0), repository);

            registerRepository(repository, repositoryType(), false, updatedRepositorySettings());

            List<RepositoryStatsSnapshot> repositoriesStatsAfterRepositoryUpdate = getRepositoriesStats();

            assertThat(repositoriesStatsAfterRepositoryUpdate.size(), equalTo(2));
            Map<String, Long> requestCounts = repositoriesStatsAfterRepositoryUpdate.get(0).requestCounts;
            assertRequestCountersAccountedForReadValues(requestCounts);
            assertRequestCountersAccountedForWriteValues(requestCounts);

            assertAllRequestCountsAreZero(repositoriesStatsAfterRepositoryUpdate.get(1));

            deleteIndex(index);

            restoreSnapshot(repository, snapshot, true);

            List<RepositoryStatsSnapshot> repoStatsAfterRestore = getRepositoriesStats();

            assertThat(repoStatsAfterRestore.size(), equalTo(2));
            assertThat(repositoriesStatsAfterRepositoryUpdate.get(0), equalTo(repoStatsAfterRestore.get(0)));

            assertRequestCountersAccountedForReadValues(repoStatsAfterRestore.get(1).requestCounts);
        });
    }

    public void testDeleteThenAddRepositoryWithTheSameName() throws Exception {
        snapshotAndRestoreIndex((repository, index) -> {
            List<RepositoryStatsSnapshot> repoStats = getRepositoriesStats();
            assertThat(repoStats.size(), equalTo(1));

            deleteRepository(repository);

            List<RepositoryStatsSnapshot> repoStatsAfterDeletion = getRepositoriesStats();
            assertThat(repoStats, equalTo(repoStatsAfterDeletion));

            registerRepository(repository, repositoryType(), false, repositorySettings());

            List<RepositoryStatsSnapshot> repositoriesStatsAfterRegisteringTheSameRepo = getRepositoriesStats();
            assertThat(repositoriesStatsAfterRegisteringTheSameRepo.size(), equalTo(2));
            assertThat(repoStats.get(0), equalTo(repositoriesStatsAfterRegisteringTheSameRepo.get(0)));
            assertAllRequestCountsAreZero(repositoriesStatsAfterRegisteringTheSameRepo.get(1));
        });
    }

    private void snapshotAndRestoreIndex(CheckedBiConsumer<String, String, Exception> biConsumer) throws Exception {
        final String snapshot = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        snapshotAndRestoreIndex(snapshot, biConsumer);
    }

    private void snapshotAndRestoreIndex(String snapshot, CheckedBiConsumer<String, String, Exception> biConsumer) throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);

        final String repositoryType = repositoryType();
        final Settings repositorySettings = repositorySettings();

        registerRepository(repository, repositoryType, true, repositorySettings);

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(1, 500);
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Document number ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", Boolean.TRUE.toString());
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));

        createSnapshot(repository, snapshot, true);

        deleteIndex(indexName);

        restoreSnapshot(repository, snapshot, true);

        biConsumer.accept(repository, indexName);
    }

    private void assertRepositoryStatsBelongToRepository(RepositoryStatsSnapshot stats, String repositoryName) {
        RepositoryId repositoryId = stats.getId();
        assertThat(repositoryId.name, equalTo(repositoryName));
        assertThat(repositoryId.type, equalTo(repositoryType()));
        assertThat(repositoryId.location, equalTo(repositoryLocation()));
    }

    private void assertAllRequestCountsAreZero(RepositoryStatsSnapshot stats) {
        for (long requestCount : stats.requestCounts.values()) {
            assertThat(requestCount, equalTo(0));
        }
    }

    private List<RepositoryStatsSnapshot> getRepositoriesStats() throws IOException {
        Map<String, Object> response = getAsMap("/_nodes/_all/_repositories_stats");
        Map<String, List<Map<String, Object>>> nodesRepoStats = extractValue(response, "nodes");
        assertThat(response.size(), greaterThan(0));
        List<RepositoryStatsSnapshot> counters = new ArrayList<>();
        for (String nodeId : getNodeIds()) {
            List<Map<String, Object>> nodeStats = nodesRepoStats.get(nodeId);
            assertThat(nodeStats, is(notNullValue()));

            for (Map<String, Object> nodeStatSnapshot : nodeStats) {
                String name = extractValue(nodeStatSnapshot, "repository_name");
                String type = extractValue(nodeStatSnapshot, "repository_type");
                String location = extractValue(nodeStatSnapshot, "repository_location");
                String createdAt = extractValue(nodeStatSnapshot, "created_at");
                Map<String, Integer> requestCounters = extractValue(nodeStatSnapshot, "request_counts");
                Map<String, Long> rc = requestCounters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().longValue()));
                Instant createdAtInstant = (Instant) DateTimeFormatter.ISO_INSTANT.parse(createdAt);
                counters.add(new RepositoryStatsSnapshot(new RepositoryId(name, type, location), rc, createdAtInstant));
            }
        }
        return counters;
    }

    private Set<String> getNodeIds() throws IOException {
        Map<String, Object> nodes = extractValue(getAsMap("_nodes/"), "nodes");
        return nodes.keySet();
    }

    private void clearRepositoriesStats() throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "/_nodes/_all/_repositories_stats");
        final Response response = client().performRequest(request);
        assertThat(
            "Failed to clear repositories stats: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    @SuppressWarnings("unchecked")
    protected static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
