/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.policy.RequestRetryOptions;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.repositories.azure.executors.PrivilegedExecutor;
import org.elasticsearch.repositories.azure.executors.ReactorScheduledExecutorService;
import org.elasticsearch.threadpool.ThreadPool;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.elasticsearch.repositories.azure.AzureRepositoryPlugin.NETTY_EVENT_LOOP_THREAD_POOL_NAME;
import static org.elasticsearch.repositories.azure.AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME;

class AzureClientProvider extends AbstractLifecycleComponent {
    public static final TimeValue DEFAULT_CONNECTION_TIMEOUT = TimeValue.timeValueSeconds(30);
    public static final TimeValue DEFAULT_MAX_CONNECTION_IDLE_TIME = TimeValue.timeValueSeconds(60);
    private static final int DEFAULT_MAX_CONNECTIONS = Math.min(Runtime.getRuntime().availableProcessors(), 8) * 2;
    private static final int DEFAULT_EVENT_LOOP_THREAD_COUNT = Math.min(Runtime.getRuntime().availableProcessors(), 8) * 2;

    final static Setting<String> EVENT_LOOP_EXECUTOR = Setting.simpleString("azure_event_loop", NETTY_EVENT_LOOP_THREAD_POOL_NAME);
    final static Setting<Integer> EVENT_LOOP_THREAD_COUNT = Setting.intSetting("azure_event_loop_thread_count",
        DEFAULT_EVENT_LOOP_THREAD_COUNT);
    final static Setting<Integer> MAX_OPEN_CONNECTIONS = Setting.intSetting("azure_max_connections", DEFAULT_MAX_CONNECTIONS);
    final static Setting<TimeValue> OPEN_CONNECTION_TIMEOUT = Setting.timeSetting("azure_connection_timeout",
        DEFAULT_CONNECTION_TIMEOUT);
    final static Setting<TimeValue> MAX_IDLE_TIME = Setting.timeSetting("azure_max_connection_idle_time",
        DEFAULT_MAX_CONNECTION_IDLE_TIME);
    final static Setting<String> REACTOR_SCHEDULER_EXECUTOR_NAME = Setting.simpleString("azure_reactor_executor_name",
        REPOSITORY_THREAD_POOL_NAME);

    private final ThreadPool threadPool;
    private final String reactorExecutorName;
    private final EventLoopGroup eventLoopGroup;
    private final ConnectionProvider connectionProvider;
    private final ByteBufAllocator byteBufAllocator;
    private volatile boolean closed = false;

    AzureClientProvider(ThreadPool threadPool,
                        String reactorExecutorName,
                        EventLoopGroup eventLoopGroup,
                        ConnectionProvider connectionProvider,
                        ByteBufAllocator byteBufAllocator) {
        this.threadPool = threadPool;
        this.reactorExecutorName = reactorExecutorName;
        this.eventLoopGroup = eventLoopGroup;
        this.connectionProvider = connectionProvider;
        this.byteBufAllocator = byteBufAllocator;
    }

    static int eventLoopThreadsFromSettings(Settings settings) {
        return EVENT_LOOP_THREAD_COUNT.get(settings);
    }

    static AzureClientProvider create(ThreadPool threadPool, Settings settings) {
        final ExecutorService executorService;
        try {
            executorService = threadPool.executor(EVENT_LOOP_EXECUTOR.get(settings));
        } catch (IllegalArgumentException e) {
            throw new SettingsException("Unable to find executor [" + EVENT_LOOP_EXECUTOR.get(settings) + "]");
        }
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(12,
            new PrivilegedExecutor(executorService));
        final TimeValue openConnectionTimeout = OPEN_CONNECTION_TIMEOUT.get(settings);
        final TimeValue maxIdleTime = MAX_IDLE_TIME.get(settings);
        ConnectionProvider provider =
            ConnectionProvider.builder("azure-sdk-connection-pool")
                .maxConnections(MAX_OPEN_CONNECTIONS.get(settings))
                .pendingAcquireTimeout(Duration.ofMillis(openConnectionTimeout.millis()))
                .maxIdleTime(Duration.ofMillis(maxIdleTime.millis()))
                .build();

        int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
        int pageSize = PooledByteBufAllocator.defaultPageSize();
        int maxOrder = PooledByteBufAllocator.defaultMaxOrder();
        int tinyCacheSize = PooledByteBufAllocator.defaultTinyCacheSize();
        int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
        int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
        boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();
        PooledByteBufAllocator pooledByteBufAllocator = new PooledByteBufAllocator(false, nHeapArena, 0, pageSize, maxOrder, tinyCacheSize,
            smallCacheSize, normalCacheSize, useCacheForAllThreads);

        String reactorExecutorName = REACTOR_SCHEDULER_EXECUTOR_NAME.get(settings);

        return new AzureClientProvider(threadPool, reactorExecutorName, eventLoopGroup, provider, pooledByteBufAllocator);
    }

    AzureBlobServiceClient createClient(AzureStorageSettings settings,
                                        LocationMode locationMode,
                                        RequestRetryOptions retryOptions,
                                        ProxyOptions proxyOptions) {
        if (closed) {
            throw new IllegalArgumentException("AzureStorageService is already closed");
        }
        reactor.netty.http.client.HttpClient nettyHttpClient = reactor.netty.http.client.HttpClient.create(connectionProvider);
        nettyHttpClient = nettyHttpClient
            .port(80)
            .wiretap(false);

        nettyHttpClient = nettyHttpClient.tcpConfiguration(tcpClient -> {
            tcpClient = tcpClient.runOn(eventLoopGroup);
            tcpClient = tcpClient.option(ChannelOption.ALLOCATOR, byteBufAllocator);
            return tcpClient;
        });

        final HttpClient httpClient = new NettyAsyncHttpClientBuilder(nettyHttpClient)
            .disableBufferCopy(false)
            .proxy(proxyOptions)
            .build();

        final String connectionString = settings.getConnectString();

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .httpClient(httpClient)
            .retryOptions(retryOptions);

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
        return new AzureBlobServiceClient(blobServiceClient, asyncClient);
    }

    @Override
    protected void doStart() {
        ReactorScheduledExecutorService executorService = new ReactorScheduledExecutorService(threadPool, reactorExecutorName);
        Schedulers.setFactory(new Schedulers.Factory() {
            @Override
            public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return Schedulers.fromExecutor(executorService);
            }

            @Override
            public Scheduler newSingle(ThreadFactory threadFactory) {
                return Schedulers.fromExecutor(executorService);
            }
        });
    }

    @Override
    protected void doStop() {
        closed = true;
        connectionProvider.dispose();
        eventLoopGroup.shutdownGracefully();
        Schedulers.resetFactory();
    }

    @Override
    protected void doClose() throws IOException {

    }
}
