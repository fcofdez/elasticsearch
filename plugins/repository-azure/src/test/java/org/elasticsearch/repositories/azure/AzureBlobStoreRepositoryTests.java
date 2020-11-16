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
package org.elasticsearch.repositories.azure;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import fixture.azure.AzureHttpHandler;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends ESMockAPIBasedRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/account",
            new AzureHTTPStatsCollectorHandler(new AzureBlobStoreHttpHandler("container")));
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new AzureBlobStoreHttpHandler("container");
    }

    @Override
    protected List<String> requestTypesTracked() {
        return List.of("GET", "LIST", "HEAD", "PUT", "PUT_BLOCK");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        String accountName = "account";
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), accountName);
        secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);

        // see com.azure.storage.blob.BlobUrlParts.parseIpUrl
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + accountName;
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * AzureRepositoryPlugin that allows to set low values for the Azure's client retry policy
     * and for BlobRequestOptions#getSingleBlobPutThresholdInBytes().
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }


        @Override
        AzureStorageService createAzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
            return new AzureStorageService(settings, azureClientProvider) {
                @Override
                RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                    return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL,
                        azureStorageSettings.getMaxRetries(), 1,
                        1L, 15L, null);
                }

                @Override
                long getUploadBlockSize() {
                    return ByteSizeUnit.MB.toBytes(1);
                }

                @Override
                long getSizeThresholdForMultiBlockUpload() {
                    return ByteSizeUnit.MB.toBytes(1);
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an Azure endpoint")
    private static class AzureBlobStoreHttpHandler extends AzureHttpHandler implements BlobStoreHttpHandler {

        AzureBlobStoreHttpHandler(final String container) {
            super(container);
        }
    }

    /**
     * HTTP handler that injects random Azure service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureErroneousHttpHandler extends ErroneousHttpHandler {

        AzureErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected void handleAsError(final HttpExchange exchange) throws IOException {
            try {
                drainInputStream(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            } finally {
                exchange.close();
            }
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            final String requestId = exchange.getRequestHeaders().getFirst("x-ms-request-id");
            final String range = exchange.getRequestHeaders().getFirst("Content-Range");
            return exchange.getRequestMethod()
                + " " + requestId
                + (range != null ? " " + range : "");
        }
    }

    /**
     * HTTP handler that keeps track of requests performed against Azure Storage.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureHTTPStatsCollectorHandler extends HttpStatsCollectorHandler {

        private static final Predicate<String> listPattern = Pattern.compile("GET /[a-zA-Z0-9]+\\??.+").asMatchPredicate();

        private AzureHTTPStatsCollectorHandler(HttpHandler delegate) {
            super(delegate);
        }

        @Override
        protected void maybeTrack(String request, Headers headers) {
            if (Regex.simpleMatch("GET /*/*", request)) {
                trackRequest("GET");
            } else if (Regex.simpleMatch("HEAD /*/*", request)) {
                trackRequest("HEAD");
            } else if (listPattern.test(request)) {
                trackRequest("LIST");
            } else if (isBlockUpload(request)) {
                trackRequest("PUT_BLOCK");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PUT");
            }
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
        private boolean isBlockUpload(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=blocklist*", request)
                || (Regex.simpleMatch("PUT /*/*?*comp=block*", request) && request.contains("blockid="));
        }
    }

    public void testBatchDeletion() throws Exception {
        // Batch API allows up to 256 per batch request
        int numberOfBlobs = randomIntBetween(257, 2000);
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(new BlobPath());
            for (int i = 0; i < numberOfBlobs; i++) {
                byte[] bytes = randomBytes(randomInt(100));
                try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
                    String blobName = randomAlphaOfLength(10);
                    container.writeBlob(blobName, inputStream, bytes.length, false);
                }
            }

            container.delete();
            assertThat(container.listBlobs().size(), equalTo(0));
        }
    }
}
