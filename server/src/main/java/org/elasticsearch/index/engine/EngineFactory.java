/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.index.store.Store;

import java.io.IOException;

/**
 * Simple Engine Factory
 */
@FunctionalInterface
public interface EngineFactory {

    Engine newReadWriteEngine(EngineConfig config);

    /**
     * Supplies the committed metadata used to initialize recovery. An engine factory may already have this metadata from a
     * relocation source, allowing checkpoint initialization and engine creation to share it without reading the store.
     */
    default SegmentInfos readLastCommittedSegmentsInfo(Store store) throws IOException {
        return store.readLastCommittedSegmentsInfo();
    }

}
