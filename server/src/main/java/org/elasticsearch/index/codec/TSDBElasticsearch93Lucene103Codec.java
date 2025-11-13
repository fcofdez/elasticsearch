/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

public class TSDBElasticsearch93Lucene103Codec extends Elasticsearch92Lucene103Codec {

    private final StoredFieldsFormat storedFieldsFormat;

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public TSDBElasticsearch93Lucene103Codec() {
        this.storedFieldsFormat = null;
    }

    /**
     * Constructor. Takes a {@link Zstd814StoredFieldsFormat.Mode} that describes whether to optimize for retrieval speed at the expense of
     * worse space-efficiency or vice-versa.
     */
    public TSDBElasticsearch93Lucene103Codec(Zstd814StoredFieldsFormat.Mode mode, boolean bloomFilterStoredFieldsEnabled) {
        super(mode);
        this.storedFieldsFormat = null;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }
}
