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

import com.azure.core.http.ProxyOptions;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.azure.storage.blob.BlobAsyncClient.BLOB_DEFAULT_NUMBER_OF_BUFFERS;
import static com.azure.storage.blob.BlobClient.BLOB_DEFAULT_UPLOAD_BLOCK_SIZE;
import static java.util.Collections.emptyMap;

public class AzureStorageService {
    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    /**
     * Maximum allowed blob size in Azure blob store.
     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    // see ModelHelper.BLOB_DEFAULT_MAX_SINGLE_UPLOAD_SIZE
    private static final long DEFAULT_MAX_SINGLE_UPLOAD_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB).getBytes();
    private static final long DEFAULT_UPLOAD_BLOCK_SIZE = BLOB_DEFAULT_UPLOAD_BLOCK_SIZE;
    private static final int DEFAULT_MAX_PARALLELISM = BLOB_DEFAULT_NUMBER_OF_BUFFERS;

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();
    private final AzureClientProvider azureClientProvider;

    public AzureStorageService(Settings settings, AzureClientProvider azureClientProvider) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshSettings(clientsSettings);
        this.azureClientProvider = azureClientProvider;
    }

    public AzureBlobServiceClient client(String clientName, LocationMode locationMode) {
        return client(clientName, locationMode, null);
    }

    public AzureBlobServiceClient client(String clientName, LocationMode locationMode, BiConsumer<String, URL> successfulRequestConsumer) {
        final AzureStorageSettings azureStorageSettings = getClientSettings(clientName);

        RequestRetryOptions retryOptions = getRetryOptions(locationMode, azureStorageSettings);
        ProxyOptions proxyOptions = getProxyOptions(azureStorageSettings);
        return azureClientProvider.createClient(azureStorageSettings, locationMode, retryOptions, proxyOptions, successfulRequestConsumer);
    }

    private AzureStorageSettings getClientSettings(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        return azureStorageSettings;
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
    long getUploadBlockSize() {
        return DEFAULT_UPLOAD_BLOCK_SIZE;
    }

    // non-static, package private for testing
    long getSizeThresholdForMultiBlockUpload() {
        return DEFAULT_MAX_SINGLE_UPLOAD_SIZE;
    }

    // non-static, package private for testing
    int getMaxUploadParallelism() {
        return DEFAULT_MAX_PARALLELISM;
    }

    int getMaxReadRetries(String clientName) {
        AzureStorageSettings azureStorageSettings = getClientSettings(clientName);
        return azureStorageSettings.getMaxRetries();
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

        // The request retry policy uses seconds as the default time unit, since
        // it's possible to configure a timeout < 1s we should ceil that value
        // as RequestRetryOptions expects a value >= 1.
        // See https://github.com/Azure/azure-sdk-for-java/issues/17590 for a proposal
        // to fix this issue.
        timeout = Math.max(1, timeout);
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL,
            azureStorageSettings.getMaxRetries(), timeout,
            null, null, secondaryHost);
    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     */
    public void refreshSettings(Map<String, AzureStorageSettings> clientsSettings) {
        this.storageSettings = Map.copyOf(clientsSettings);
        // clients are built lazily by {@link client(String, LocationMode)}
    }
}
