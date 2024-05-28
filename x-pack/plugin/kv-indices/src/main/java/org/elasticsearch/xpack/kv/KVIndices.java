/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.kv;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class KVIndices extends Plugin implements ActionPlugin, EnginePlugin {

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(RocksEngine.INDEX_KV)) {
            return Optional.of(RocksEngine::new);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(RocksEngine.INDEX_KV);
    }
}
