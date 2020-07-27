/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;

public abstract class AbstractRepositoriesStatsAPIRestTestCase extends ESRestTestCase {
    protected abstract String repositoryType();

    protected abstract Settings repositorySettings();

    protected abstract Settings updatedRepositorySettings();

//    private void testRepositoryStats() throws Exception {
//        createIndex(randomAlphaOfLength(10), Settings.EMPTY);
//
//        String repositoryName = randomAlphaOfLength(10);
//        registerRepository(repositoryName, repositoryType(), false, repositorySettings());
//
//        final Request request = new Request(HttpPut.METHOD_NAME, "/_nodes/_all/_repositories_stats");
//        final Response response = client().performRequest(request);
//        Map<String, Object> responseAsMap = responseAsMap(response);
//        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
//    }
//
//    public void testRegisterAndDeleteARepositoryKeepStatsAround() throws Exception {
//        createIndex(randomAlphaOfLength(10), Settings.EMPTY);
//
//        String repositoryName = randomAlphaOfLength(10);
//        registerRepository(repositoryName, repositoryType(), false, repositorySettings());
//
//        final Response response = getRepositoriesStats();
//        Map<String, Object> responseAsMap = responseAsMap(response);
//        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
//        deleteRepository(repositoryName);
//
//        Response repositoriesStats = getRepositoriesStats();
//    }
//
//    public void testUpdateRepository() throws Exception {
//        createIndex(randomAlphaOfLength(10), Settings.EMPTY);
//
//        String repositoryName = randomAlphaOfLength(10);
//        registerRepository(repositoryName, repositoryType(), false, repositorySettings());
//
//        final Response response = getRepositoriesStats();
//        Map<String, Object> responseAsMap = responseAsMap(response);
//        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
//        deleteRepository(repositoryName);
//
//        registerRepository(repositoryName, repositoryType(), false, updatedRepositorySettings());
//
//        Response repositoriesStats = getRepositoriesStats();
//    }
//
    @SuppressWarnings("unchecked")
    public void testStatsAreTracked() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);

        final String repositoryType = repositoryType();
        final Settings repositorySettings = repositorySettings();

        final String repository = "repository";
        logger.info("creating repository [{}] of type [{}]", repository, repositoryType);
        registerRepository(repository, repositoryType, true, repositorySettings);

        logger.info("creating index [{}]", indexName);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(1, 500);
        logger.info("indexing [{}] documents", numDocs);

        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Document number ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", Boolean.TRUE.toString());
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));

        if (randomBoolean()) {
            final StringBuilder bulkUpdateBody = new StringBuilder();
            for (int i = 0; i < randomIntBetween(1, numDocs); i++) {
                bulkUpdateBody.append("{\"update\":{\"_id\":\"").append(i).append("\"}}\n");
                bulkUpdateBody.append("{\"doc\":{").append("\"text\":\"Updated document number ").append(i).append("\"}}\n");
            }

            final Request bulkUpdate = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
            bulkUpdate.addParameter("refresh", Boolean.TRUE.toString());
            bulkUpdate.setJsonEntity(bulkUpdateBody.toString());
            assertOK(client().performRequest(bulkUpdate));
        }

        final String snapshot = "test-snapshot";

        // Remove the snapshots, if a previous test failed to delete them. This is
        // useful for third party tests that runs the test against a real external service.
        deleteSnapshot(repository, snapshot, true);

        logger.info("creating snapshot [{}]", snapshot);
        createSnapshot(repository, snapshot, true);

        logger.info("deleting index [{}]", indexName);
        deleteIndex(indexName);

        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        logger.info("restoring index [{}] from snapshot [{}] as [{}]", indexName, snapshot, restoredIndexName);

        restoreSnapshot(repository, snapshot, true);

        Response repositoriesStats = getRepositoriesStats();
        Map<String, Object> response = responseAsMap(repositoriesStats);
        assertThat(response.size(), greaterThan(0));
        for (String nodeId : getNodeIds()) {
            List<Object> nodeStats = extractValue(response, nodeId);
            assertThat(nodeStats, is(notNullValue()));
            for (Object nodeStatSnapshot : nodeStats) {
                // TODO: is there a better way to extract this info?
                Map<String, Object> stats = (Map<String, Object>) nodeStatSnapshot;
                Map<String, Integer> requestCounters = extractValue(stats, "request_counts");
                assertThat(requestCounters.get("GET"), is(greaterThan(0)));
                assertThat(requestCounters.get("PUT"), is(greaterThan(0)));
                assertThat(requestCounters.get("LIST"), is(greaterThan(0)));
            }
        }
    }

    private Set<String> getNodeIds() throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "_nodes/");
        final Response resp = client().performRequest(request);
        Map<String, Object> nodes = extractValue(responseAsMap(resp), "nodes");
        return nodes.keySet();
    }

    protected static void createSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    protected static void deleteSnapshot(String repository, String snapshot, boolean ignoreMissing) throws IOException {
        final Request request = new Request(HttpDelete.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        try {
            final Response response = client().performRequest(request);
            assertAcked("Failed to delete snapshot [" + snapshot + "] in repository [" + repository + "]: " + response, response);
        } catch (IOException e) {
            if (ignoreMissing && e instanceof ResponseException) {
                Response response = ((ResponseException) e).getResponse();
                assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
                return;
            }
            throw e;
        }
    }

    protected static void restoreSnapshot(String repository, String snapshot, boolean waitForCompletion) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot + "/_restore");
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));

        final Response response = client().performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + snapshot + "] from repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    private Response getRepositoriesStats() throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_nodes/_all/_repositories_stats");
        final Response response = client().performRequest(request);
        assertThat(
            "Failed to get repositories stats: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
        return response;
    }

    protected static void registerRepository(String repository, String type, boolean verify, Settings settings) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository);
        request.setJsonEntity(Strings.toString(new PutRepositoryRequest(repository).type(type).verify(verify).settings(settings)));

        final Response response = client().performRequest(request);
        assertAcked("Failed to create repository [" + repository + "] of type [" + type + "]: " + response, response);
    }

    private static void assertAcked(String message, Response response) throws IOException {
        final int responseStatusCode = response.getStatusLine().getStatusCode();
        assertThat(
            message + ": expecting response code [200] but got [" + responseStatusCode + ']',
            responseStatusCode,
            equalTo(RestStatus.OK.getStatus())
        );
        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(message + ": response is not acknowledged", extractValue(responseAsMap, "acknowledged"), equalTo(Boolean.TRUE));
    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        final XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        assertThat("Unknown XContentType", xContentType, notNullValue());

        BytesReference bytesReference = Streams.readFully(response.getEntity().getContent());

        try (InputStream responseBody = bytesReference.streamInput()) {
            return XContentHelper.convertToMap(xContentType.xContent(), responseBody, true);
        } catch (Exception e) {
            throw new IOException(bytesReference.utf8ToString(), e);
        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
