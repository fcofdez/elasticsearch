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

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

public class AzureStorageService {

    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();
    private volatile Map<Tuple<AzureStorageSettings, LocationMode>, AzureBlobServiceClient> clients = Collections.emptyMap();
    private final AzureClientProvider azureClientProvider;

    public AzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshSettings(clientsSettings);
        this.azureClientProvider = azureClientProvider;
    }

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public AzureBlobServiceClient client(String clientName, LocationMode locationMode) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }

        RequestRetryOptions retryOptions = getRetryOptions(locationMode, azureStorageSettings);
        ProxyOptions proxyOptions = getProxyOptions(azureStorageSettings);
        return azureClientProvider.createClient(azureStorageSettings, locationMode, retryOptions, proxyOptions);
    }

    long getUploadBlockSize() {
        return Math.toIntExact(ByteSizeUnit.MB.toBytes(1));
    }

    int getMaxUploadParallelism() {
        return 5;
    }

    public interface ReqInfoCollector {
        void collect(String requestMethod, URL requestURL, int responseStatus);
    }

    private static final class RequestCollector implements HttpPipelinePolicy {
        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
            Optional<Object> collector = context.getData("collector");
            if (collector.isEmpty() || collector.get() instanceof ReqInfoCollector == false) {
                return next.process();
            }

            ReqInfoCollector reqInfoCollector = (ReqInfoCollector) collector.get();
            return next.process()
                .doOnSuccess(httpResponse -> {
                    HttpRequest httpRequest = context.getHttpRequest();
                    try {
                        reqInfoCollector.collect(httpRequest.getHttpMethod().name(), httpRequest.getUrl(), httpResponse.getStatusCode());
                    } catch (Exception e) {
                        // Ignore
                    }
                });
        }
    }

    private static ProxyOptions getProxyOptions(AzureStorageSettings settings) {
        Proxy proxy = settings.getProxy();
        if (proxy == null) {
            return null;
        }

        switch (proxy.type()) {
            case HTTP:
                return new ProxyOptions(ProxyOptions.Type.HTTP, (InetSocketAddress) proxy.address());
            case SOCKS:
                return new ProxyOptions(ProxyOptions.Type.SOCKS5, (InetSocketAddress) proxy.address());
            default:
                return null;
        }
    }

    // non-static, package private for testing
    RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
        int timeout = Math.toIntExact(azureStorageSettings.getTimeout().getSeconds());
        String connectString = azureStorageSettings.getConnectString();
        StorageConnectionString storageConnectionString = StorageConnectionString.create(connectString, null);
        String primaryUri = storageConnectionString.getBlobEndpoint().getPrimaryUri();
        String secondaryUri = storageConnectionString.getBlobEndpoint().getSecondaryUri();

        if (locationMode.isSecondary() && secondaryUri == null) {
            throw new IllegalArgumentException("Expected to get a secondary URI");
        }

        final String secondaryHost;
        switch (locationMode) {
            case PRIMARY_ONLY:
            case SECONDARY_ONLY:
                secondaryHost = null;
                break;
            case PRIMARY_THEN_SECONDARY:
                secondaryHost = secondaryUri;
                break;
            case SECONDARY_THEN_PRIMARY:
                secondaryHost = primaryUri;
                break;
            default:
                throw new IllegalStateException();
        }

        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL,
            azureStorageSettings.getMaxRetries(), 1,
            1L, 15L, secondaryHost);
    }

    long uploadChunkSize() {
        return ByteSizeUnit.MB.toBytes(1);
    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     * @return the old settings
     */
    public void refreshSettings(Map<String, AzureStorageSettings> clientsSettings) {
        this.storageSettings = Map.copyOf(clientsSettings);
        // clients are built lazily by {@link client(String, LocationMode)}
    }
}
