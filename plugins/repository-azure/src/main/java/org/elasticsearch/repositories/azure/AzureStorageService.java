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
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.threadpool.ThreadPool;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private volatile Map<Tuple<AzureStorageSettings, LocationMode>, AzureBlobServiceClientRef> clients = Collections.emptyMap();

    /**
     * Creates a {@code CloudBlobClient} on each invocation using the current client
     * settings. CloudBlobClient is not thread safe and the settings can change,
     * therefore the instance is not cache-able and should only be reused inside a
     * thread for logically coupled ops. The {@code OperationContext} is used to
     * specify the proxy, but a new context is *required* for each call.
     */
    public AzureBlobServiceClientRef client(String clientName, LocationMode locationMode, ThreadPool threadPool) {
        final AzureStorageSettings azureStorageSettings = this.storageSettings.get(clientName);
        if (azureStorageSettings == null) {
            throw new SettingsException("Unable to find client with name [" + clientName + "]");
        }

        Tuple<AzureStorageSettings, LocationMode> settings = new Tuple<>(azureStorageSettings, locationMode);
        try {
            {
                final AzureBlobServiceClientRef blobServiceClientRef = clients.get(settings);
                if (blobServiceClientRef != null && blobServiceClientRef.tryIncRef()) {
                    return blobServiceClientRef;
                }
            }

            synchronized (this) {
                final AzureBlobServiceClientRef existing = clients.get(settings);
                if (existing != null && existing.tryIncRef()) {
                    return existing;
                }
                final AzureBlobServiceClientRef blobServiceClientRef = createClientReference(clientName, locationMode, azureStorageSettings,
                    threadPool);
                blobServiceClientRef.incRef();
                clients = Maps.copyMapWithAddedEntry(clients, settings, blobServiceClientRef);
                return blobServiceClientRef;
            }
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

    /**
     * Executor that gives permissions to runnables
     */
    private static class PrivilegedExecutor implements Executor {
        private final Executor delegate;

        private PrivilegedExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(() -> {
                SocketAccess.doPrivilegedVoidException(command::run);
            });
        }
    }

    private final static AtomicBoolean hackSetUp = new AtomicBoolean(false);

    private AzureBlobServiceClientRef createClientReference(String clientName, LocationMode locationMode, AzureStorageSettings settings, ThreadPool threadPool) {
        if (hackSetUp.compareAndSet(false, true)) {
            Schedulers.setFactory(new Schedulers.Factory() {
                @Override
                public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
                    return Schedulers.fromExecutor(threadPool.scheduler());
                }

                @Override
                public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
                    return Schedulers.fromExecutor(threadPool.executor(ThreadPool.Names.SNAPSHOT));
                }

                @Override
                public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                    return Schedulers.fromExecutor(threadPool.executor(ThreadPool.Names.SNAPSHOT));
                }

                @Override
                public Scheduler newSingle(ThreadFactory threadFactory) {
                    return Schedulers.fromExecutor(threadPool.executor(ThreadPool.Names.SNAPSHOT));
                }
            });
        }

        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(2,
            new PrivilegedExecutor(threadPool.executor(ThreadPool.Names.GENERIC)));
        final HttpClient httpClient = new NettyAsyncHttpClientBuilder().eventLoopGroup(eventLoopGroup)
            .disableBufferCopy(true)
            .proxy(getProxyOptions(settings))
            .build();

        final String connectionString = settings.getConnectString();

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .httpClient(httpClient)
            .retryOptions(getRetryOptions(locationMode, settings));

        if (locationMode.isSecondary()) {
            StorageConnectionString storageConnectionString = StorageConnectionString.create(connectionString, null);
            String secondaryUri = storageConnectionString.getBlobEndpoint().getSecondaryUri();
            if (secondaryUri == null) {
                throw new IllegalArgumentException();
            }

            builder.endpoint(secondaryUri);
        }

        return new AzureBlobServiceClientRef(clientName, SocketAccess.doPrivilegedException(builder::buildClient), eventLoopGroup);
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
        // TODO, throw a proper exception here?
        int timeout = Math.toIntExact(azureStorageSettings.getTimeout().getMillis());
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
            azureStorageSettings.getMaxRetries(), timeout == -1 ? null : timeout,
            null, null, secondaryHost);
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
        releaseCachedClients();
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

    void close() {
        releaseCachedClients();
    }

    private void releaseCachedClients() {
        for (AzureBlobServiceClientRef clientRef : clients.values()) {
            clientRef.decRef();
        }
        clients = Collections.emptyMap();
    }

    // package private for testing
//    BlobRequestOptions getBlobRequestOptionsForWriteBlob() {
//        return null;
//    }
}
