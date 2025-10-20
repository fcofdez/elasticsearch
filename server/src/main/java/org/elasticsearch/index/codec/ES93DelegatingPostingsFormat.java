/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.lucene.SyntheticIdFieldsProducer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

public class ES93DelegatingPostingsFormat extends PostingsFormat {
    public static final String FORMAT_NAME = "ES93DelegatingPostings";

    public ES93DelegatingPostingsFormat() {
        super(FORMAT_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        assert false;
        return null;
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer docValuesProducer = null;
        boolean success = false;
        try {
            var codec = state.segmentInfo.getCodec();
            // Hack: The provided SegmentReadState uses the ES87BloomFilter suffix for filenames, while the tsids
            // won't have that. Just use an empty suffix to circumvent this for now.
            docValuesProducer = codec.docValuesFormat().fieldsProducer(new SegmentReadState(state, ""));
            var fieldsProducer = new SyntheticIdFieldsProducer(state, docValuesProducer);
            success = true;
            return fieldsProducer;
        } finally {
            if (success == false) {
                IOUtils.close(docValuesProducer);
            }
        }
    }
}
