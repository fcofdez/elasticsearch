/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MapperServiceFactory {

    public static MapperService create(String mappings) {
        return create(mappings, Collections.emptyList());
    }

    public static MapperService create(String mappings, List<MapperPlugin> mapperPlugins) {
        return create(mappings, mapperPlugins, Settings.EMPTY);
    }

    /**
     * Creates a {@link MapperService} for the given mappings, plugins, and extra index settings.
     * The {@code extraSettings} are merged last, so they override any defaults set by this factory
     * (e.g. {@code index.mode}, {@code index.mapping.source.mode}).
     */
    public static MapperService create(String mappings, List<MapperPlugin> mapperPlugins, Settings extraSettings) {
        final CompressedXContent source;
        try {
            source = new CompressedXContent(mappings);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.mapping.total_fields.limit", 100000)
            .put(extraSettings)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        MapperService mapperService = createValidationService(meta, new IndicesModule(mapperPlugins).getMapperRegistry());
        try {
            mapperService.merge("_doc", source, MapperService.MergeReason.MAPPING_UPDATE);
            return mapperService;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(mapperService);
            throw e;
        }
    }

    /**
     * Creates an unmerged validation service with a shared registry so benchmarks can load mappings outside measured operations.
     * The caller owns the service and must close it.
     */
    public static MapperService createValidationService(IndexMetadata meta, MapperRegistry mapperRegistry) {
        Settings settings = meta.getSettings();
        IndexSettings indexSettings = new IndexSettings(meta, settings);

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP);
        return new MapperService(
            () -> TransportVersion.current(),
            f -> true,
            indexSettings,
            IndexAnalyzers.of(
                Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
                Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()))
            ),
            XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()))
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            similarityService,
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException();
            },
            () -> true,
            new ScriptCompiler() {
                @Override
                public <T> T compile(Script script, ScriptContext<T> scriptContext) {
                    throw new UnsupportedOperationException();
                }
            },
            bitsetFilterCache::getBitSetProducer,
            MapperMetrics.NOOP,
            null,
            null
        );
    }
}
