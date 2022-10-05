/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class DataStreamsWriteLoadSamplerPlugin extends Plugin {
    public static final String WRITE_LOAD_SAMPLING_THREAD_POOL_NAME = "write_load_sampler";
    public static final String INDICES_WRITE_LOAD_SAMPLING_THREAD_POOL_PREFIX = "xpack.indices_write_load_sampling_thread_pool";
    private final SetOnce<DataStreamsWriteLoadSampler> indicesWriteLoadsStatsSamplerRef = new SetOnce<>();

    public DataStreamsWriteLoadSamplerPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationDeciders allocationDeciders
    ) {
        final var indicesWriteLoadStatsCollector = new DataStreamsWriteLoadSampler(
            clusterService::state,
            threadPool::rawRelativeTimeInNanos
        );
        indicesWriteLoadsStatsSamplerRef.set(indicesWriteLoadStatsCollector);

        return Collections.emptyList();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        assert indicesWriteLoadsStatsSamplerRef.get() != null;
        indexModule.addIndexEventListener(indicesWriteLoadsStatsSamplerRef.get());
    }

    static boolean currentThreadIsWriterLoadSamplerThreadOrTestThread() {
        return Thread.currentThread().getName().contains('[' + WRITE_LOAD_SAMPLING_THREAD_POOL_NAME + ']')
            || Thread.currentThread().getName().startsWith("TEST-");
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(
            new FixedExecutorBuilder(
                settings,
                WRITE_LOAD_SAMPLING_THREAD_POOL_NAME,
                1,
                100,
                INDICES_WRITE_LOAD_SAMPLING_THREAD_POOL_PREFIX,
                false
            )
        );
    }
}
