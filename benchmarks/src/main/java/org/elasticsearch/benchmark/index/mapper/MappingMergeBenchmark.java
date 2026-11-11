/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of merging a fixed set of distinct dynamic mapping updates into an already loaded mapping, individually or together.
 * One operation applies every update; it excludes preflight checks, task execution, and cluster-state publication. Service creation,
 * recovery, verification, and closing are outside timing, but their allocations are included by JMH's GC profiler.
 */
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@State(Scope.Thread)
public class MappingMergeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "100", "10000" })
    public int existingFields;

    @Param({ "1", "100" })
    public int updateCount;

    @Param({ "false", "true" })
    public boolean batching;

    private MapperRegistry mapperRegistry;
    private IndexMetadata initialMetadata;
    private List<CompressedXContent> updates;
    private CompressedXContent expectedSource;
    private MapperService service;

    /** Checks that both strategies produce the same mapping with every requested field before measuring either strategy. */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (existingFields < 0 || updateCount < 1) {
            throw new IllegalArgumentException("existing field count must be nonnegative and update count must be positive");
        }
        mapperRegistry = new IndicesModule(List.of()).getMapperRegistry();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), existingFields + updateCount)
            .build();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().startObject(MapperService.SINGLE_MAPPING_NAME).startObject("properties");
            for (int i = 0; i < existingFields; i++) {
                builder.startObject("existing" + i).field("type", "keyword").endObject();
            }
            builder.endObject().endObject().endObject();
            initialMetadata = IndexMetadata.builder("index").settings(settings).putMapping(Strings.toString(builder)).build();
        }
        List<CompressedXContent> sources = new ArrayList<>(updateCount);
        for (int i = 0; i < updateCount; i++) {
            sources.add(new CompressedXContent("{\"properties\":{\"added" + i + "\":{\"type\":\"keyword\"}}}"));
        }
        updates = List.copyOf(sources);
        try (MapperService sequential = createService(); MapperService batched = createService()) {
            expectedSource = merge(sequential, false).mappingSource();
            merge(batched, true);
            verify(sequential);
            verify(batched);
            for (int i = 0; i < updateCount; i++) {
                if (sequential.fieldType("added" + i) == null || batched.fieldType("added" + i) == null) {
                    throw new IllegalStateException("missing added field " + i);
                }
            }
        }
    }

    private MapperService createService() {
        MapperService mapperService = MapperServiceFactory.createValidationService(initialMetadata, mapperRegistry);
        try {
            mapperService.merge(initialMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            return mapperService;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(mapperService);
            throw e;
        }
    }

    /** Starts each operation with the same mapping so repeated invocations cannot become no-ops. */
    @Setup(Level.Invocation)
    public void prepareInvocation() {
        service = createService();
    }

    /** Applies all updates, including parsing, merging, validation, and serialization of the resulting mapper. */
    @Benchmark
    public DocumentMapper merge() {
        return merge(service, batching);
    }

    private DocumentMapper merge(MapperService mapperService, boolean batched) {
        if (batched) {
            return mapperService.mergeDynamicMappingUpdates(updates);
        }
        for (CompressedXContent update : updates) {
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, update, MapperService.MergeReason.MAPPING_AUTO_UPDATE);
        }
        return mapperService.documentMapper();
    }

    private void verify(MapperService mapperService) {
        if (mapperService.mappingLookup().getTotalFieldsCount() != existingFields + updateCount
            || expectedSource.equals(mapperService.documentMapper().mappingSource()) == false) {
            throw new IllegalStateException("unexpected final mapping");
        }
    }

    /** Checks the result and releases the service after the measured operation. */
    @TearDown(Level.Invocation)
    public void finishInvocation() throws IOException {
        try {
            verify(service);
        } finally {
            service.close();
        }
    }
}
