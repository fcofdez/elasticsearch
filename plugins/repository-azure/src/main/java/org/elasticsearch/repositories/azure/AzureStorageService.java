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

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;

public class AzureStorageService {

    public static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
//    /**
//     * {@link com.microsoft.azure.storage.blob.BlobConstants#MAX_SINGLE_UPLOAD_BLOB_SIZE_IN_BYTES}
//     */
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(256, ByteSizeUnit.MB);

    // 'package' for testing
    volatile Map<String, AzureStorageSettings> storageSettings = emptyMap();

    public AzureStorageService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, AzureStorageSettings> clientsSettings = AzureStorageSettings.load(settings);
        refreshAndClearCache(clientsSettings);
    }

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public BlobServiceClient client(String clientName) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }
        try {
            return createClient(azureStorageSettings);
        } catch (IllegalArgumentException e) {
            throw new SettingsException("Invalid azure client settings with name [" + clientName + "]", e);
        }
    }

//    private BlobServiceClient buildClient(AzureStorageSettings azureStorageSettings) throws InvalidKeyException, URISyntaxException {
//        final BlobServiceClient client = createClient(azureStorageSettings);
//        // Set timeout option if the user sets cloud.azure.storage.timeout or
//        // cloud.azure.storage.xxx.timeout (it's negative by default)
//        final long timeout = azureStorageSettings.getTimeout().getMillis();
//        if (timeout > 0) {
//            if (timeout > Integer.MAX_VALUE) {
//                throw new IllegalArgumentException("Timeout [" + azureStorageSettings.getTimeout() + "] exceeds 2,147,483,647ms.");
//            }
//            client.getDefaultRequestOptions().setTimeoutIntervalInMs((int) timeout);
//        }
//        // We define a default exponential retry policy
//        client.getDefaultRequestOptions().setRetryPolicyFactory(createRetryPolicy(azureStorageSettings));
//        client.getDefaultRequestOptions().setLocationMode(azureStorageSettings.getLocationMode());
//        return client;
//    }

    // non-static, package private for testing
    RequestRetryOptions createRetryPolicy(final AzureStorageSettings azureStorageSettings) {
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, azureStorageSettings.getMaxRetries(), Math.toIntExact(azureStorageSettings.getTimeout().getMillis()), null, null, null) ;
    }

    static class EsThreadFactory implements ThreadFactory {

        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        EsThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                namePrefix + "[T#" + threadNumber.getAndIncrement() + "]",
                0);
            t.setDaemon(true);
            return t;
        }
    }


    private static BlobServiceClient createClient(AzureStorageSettings azureStorageSettings) {
        final String connectionString = azureStorageSettings.getConnectString();
        final StorageSharedKeyCredential credential = StorageSharedKeyCredential.fromConnectionString(connectionString);
        NettyAsyncHttpClientBuilder httpClientBuilder = new NettyAsyncHttpClientBuilder();
        //TODO
//        new ProxyOptions()
//        httpClientBuilder.proxy()
        NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(2, new EsThreadFactory("azure"));
        HttpClient httpClient = httpClientBuilder.eventLoopGroup(nioEventLoopGroup).build();

        return new BlobServiceClientBuilder()
            .credential(credential)
            .httpClient(httpClient)
            .retryOptions(new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, azureStorageSettings.getMaxRetries(), Math.toIntExact(azureStorageSettings.getTimeout().getMillis()), null, null, null))
            .buildClient();
    }

//    private static OperationContext buildOperationContext(AzureStorageSettings azureStorageSettings) {
//        final OperationContext context = new OperationContext();
//        context.setProxy(azureStorageSettings.getProxy());
//        return context;
//    }

    /**
     * Updates settings for building clients. Any client cache is cleared. Future
     * client requests will use the new refreshed settings.
     *
     * @param clientsSettings the settings for new clients
     * @return the old settings
     */
    public Map<String, AzureStorageSettings> refreshAndClearCache(Map<String, AzureStorageSettings> clientsSettings) {
        final Map<String, AzureStorageSettings> prevSettings = this.storageSettings;
        this.storageSettings = Map.copyOf(clientsSettings);
        // clients are built lazily by {@link client(String)}
        return prevSettings;
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    static String blobNameFromUri(URI uri) {
        final String path = uri.getPath();
        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        final String[] splits = path.split("/", 3);
        // We return the remaining end of the string
        return splits[2];
    }

    // package private for testing
//    BlobRequestOptions getBlobRequestOptionsForWriteBlob() {
//        return null;
//    }
}
