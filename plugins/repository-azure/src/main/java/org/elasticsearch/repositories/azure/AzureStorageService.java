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
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.threadpool.ThreadPool;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    long getUploadBlockSize() {
        return Math.toIntExact(ByteSizeUnit.MB.toBytes(1));
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

    private static final AtomicBoolean hackSetUp = new AtomicBoolean(false);

    private final static class PrivilegedScheduledEx implements ScheduledExecutorService {
        private final ScheduledExecutorService delegate;

        public PrivilegedScheduledEx(ScheduledExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            Callable<V> privilegedCallable = () -> SocketAccess.doPrivilegedException(callable::call);
            return delegate.schedule(privilegedCallable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            Runnable privilegedCommand = () -> SocketAccess.doPrivilegedVoidException(command::run);
            return delegate.scheduleAtFixedRate(privilegedCommand, initialDelay, period, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            Callable<T> privilegedTask = () -> SocketAccess.doPrivilegedException(task::call);
            return delegate.submit(privilegedTask);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return delegate.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        private final Logger logger = LogManager.getLogger(PrivilegedScheduledEx.class);
        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }


        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return delegate.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return delegate.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return delegate.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }

    static class LoggerEventLoopGroup extends NioEventLoopGroup {
        private final Logger logger = LogManager.getLogger(LoggerEventLoopGroup.class);

        public LoggerEventLoopGroup(int nThreads, Executor executor) {
            super(nThreads, executor);
        }

        @Override
        public ChannelFuture register(Channel channel) {
            logger.info("Register channel {}", channel);
            return super.register(channel);
        }

        @Override
        public ChannelFuture register(ChannelPromise promise) {
            logger.info("Register channel {}", promise);
            return super.register(promise);
        }
    }

    static class HttpCl implements HttpClient {
        private final HttpClient delegate;
        private final Logger logger = LogManager.getLogger(HttpCl.class);

        public HttpCl(HttpClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            logger.info("Sending http req {} {} / {}", request.getHttpMethod(), request.getUrl(), Thread.currentThread().getStackTrace());
            Mono<HttpResponse> send = delegate.send(request);
            logger.info("Request sent");
            return send;
        }
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

    private AzureBlobServiceClientRef createClientReference(String clientName, LocationMode locationMode, AzureStorageSettings settings, ThreadPool threadPool) {
        if (hackSetUp.compareAndSet(false, true)) {
            Schedulers.setFactory(new Schedulers.Factory() {
                @Override
                public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
                    return Schedulers.fromExecutor(new PrivilegedScheduledEx(threadPool.scheduler()));
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

        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(12,
            new PrivilegedExecutor(threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME)));
        ConnectionProvider provider =
            ConnectionProvider.builder("fixed")
                .maxConnections(50)
                .pendingAcquireTimeout(Duration.ofMillis(30000))
                .maxIdleTime(Duration.ofMinutes(60))
                .build();

        reactor.netty.http.client.HttpClient nettyHttpClient = reactor.netty.http.client.HttpClient.create(provider);
        nettyHttpClient = nettyHttpClient
            .port(80)
            .wiretap(false);

        int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
        int pageSize = PooledByteBufAllocator.defaultPageSize();
        int maxOrder = PooledByteBufAllocator.defaultMaxOrder();
        int tinyCacheSize = PooledByteBufAllocator.defaultTinyCacheSize();
        int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
        int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
        boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();
        PooledByteBufAllocator pooledByteBufAllocator = new PooledByteBufAllocator(false, nHeapArena, 0, pageSize, maxOrder, tinyCacheSize,
            smallCacheSize, normalCacheSize, useCacheForAllThreads);

        nettyHttpClient = nettyHttpClient.tcpConfiguration(tcpClient -> {
            tcpClient = tcpClient.runOn(eventLoopGroup);
            tcpClient = tcpClient.option(ChannelOption.ALLOCATOR, pooledByteBufAllocator);
            return tcpClient;
        });

        final HttpClient httpClient = new NettyAsyncHttpClientBuilder(nettyHttpClient)
            .disableBufferCopy(false)
            .proxy(getProxyOptions(settings))
            .connectionProvider(provider)
            .build();

        HttpPipeline build = new HttpPipelineBuilder()
            .httpClient(httpClient)
            .policies(new RequestRetryPolicy(getRetryOptions(locationMode, settings)))
            .build();


        final String connectionString = settings.getConnectString();

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .pipeline(build)
            .retryOptions(getRetryOptions(locationMode, settings));

        if (locationMode.isSecondary()) {
            StorageConnectionString storageConnectionString = StorageConnectionString.create(connectionString, null);
            String secondaryUri = storageConnectionString.getBlobEndpoint().getSecondaryUri();
            if (secondaryUri == null) {
                throw new IllegalArgumentException();
            }

            builder.endpoint(secondaryUri);
        }

        BlobServiceClient blobServiceClient = SocketAccess.doPrivilegedException(builder::buildClient);
        BlobServiceAsyncClient asyncClient = SocketAccess.doPrivilegedException(builder::buildAsyncClient);
        return new AzureBlobServiceClientRef(clientName, blobServiceClient, asyncClient, eventLoopGroup, settings.getTimeout(), provider);
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

    void close() {
        hackSetUp.compareAndSet(true, false);
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
