/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.engine.kv;

import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.kv.KVIndices;
import org.elasticsearch.xpack.kv.RocksEngine;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
@ESTestCase.WithoutSecurityManager
public class KVIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(KVIndices.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testBasic() throws Exception {
        internalCluster().startNodes(1);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(RocksEngine.INDEX_KV.getKey(), true).build());
        indexDoc(indexName, "test", "asd", "asd1");
        indexDoc(indexName, "test2", "asd", "asd1");
        RocksEngine engine = (RocksEngine) internalCluster().getInstance(IndicesService.class)
            .indexService(resolveIndex(indexName))
            .getShard(0)
            .getEngineOrNull();

        assertThat(engine.getBytes("test".getBytes()), notNullValue());
        assertThat(engine.getBytes("test2".getBytes()), notNullValue());
    }
}
