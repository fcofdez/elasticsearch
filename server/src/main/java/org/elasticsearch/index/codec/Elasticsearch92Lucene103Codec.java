/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.codecs.perfield.PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY;
import static org.apache.lucene.codecs.perfield.PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY;

/**
 * Elasticsearch codec as of 9.2 relying on Lucene 10.3. This extends the Lucene 10.3 codec to compressed
 * stored fields with ZSTD instead of LZ4/DEFLATE. See {@link Zstd814StoredFieldsFormat}.
 */
public class Elasticsearch92Lucene103Codec extends CodecService.DeduplicateFieldInfosCodec {

    static final PostingsFormat DEFAULT_POSTINGS_FORMAT = new Lucene103PostingsFormat();

    private final StoredFieldsFormat storedFieldsFormat;

    private final PostingsFormat defaultPostingsFormat;
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Elasticsearch92Lucene103Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat;
    private final DocValuesFormat docValuesFormat = new XPerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Elasticsearch92Lucene103Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat;
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return Elasticsearch92Lucene103Codec.this.getKnnVectorsFormatForField(field);
        }
    };

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch92Lucene103Codec() {
        this(Zstd814StoredFieldsFormat.Mode.BEST_SPEED);
    }

    /**
     * Constructor. Takes a {@link Zstd814StoredFieldsFormat.Mode} that describes whether to optimize for retrieval speed at the expense of
     * worse space-efficiency or vice-versa.
     */
    public Elasticsearch92Lucene103Codec(Zstd814StoredFieldsFormat.Mode mode) {
        super("Elasticsearch92Lucene103", new Lucene103Codec());
        this.storedFieldsFormat = mode.getFormat();
        this.defaultPostingsFormat = DEFAULT_POSTINGS_FORMAT;
        this.defaultDVFormat = new Lucene90DocValuesFormat();
        this.defaultKnnVectorsFormat = new Lucene99HnswVectorsFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The default implementation always returns "Lucene912".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation,
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultPostingsFormat;
    }

    /**
     * Returns the docvalues format that should be used for writing new segments of <code>field</code>
     * .
     *
     * <p>The default implementation always returns "Lucene912".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    /**
     * Returns the vectors format that should be used for writing new segments of <code>field</code>
     *
     * <p>The default implementation always returns "Lucene912".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return new FakeIdFieldInfosFormat(super.fieldInfosFormat());
    }

    static class FakeIdFieldInfosFormat extends FieldInfosFormat {
        private final FieldInfosFormat delegate;

        FakeIdFieldInfosFormat(FieldInfosFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            var fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
            if (fieldInfos.fieldInfo(IdFieldMapper.NAME) != null) {
                return fieldInfos;
            }

            FieldInfo[] fieldInfosArray = new FieldInfo[fieldInfos.size() + 1];
            for (FieldInfo fieldInfo : fieldInfos) {
                fieldInfosArray[fieldInfo.getFieldNumber()] = fieldInfo;
            }

            Map<String, String> attributes = new HashMap<>();
            if (segmentInfo.getCodec() instanceof PerFieldMapperCodec codec) {
                var postingsFormat = ES93DelegatingPostingsFormat.FORMAT_NAME;

                attributes.put(PER_FIELD_FORMAT_KEY, postingsFormat);
                attributes.put(PER_FIELD_SUFFIX_KEY, Integer.toString(0));
            }

            fieldInfosArray[fieldInfosArray.length - 1] = new FieldInfo(
                IdFieldMapper.NAME,
                fieldInfosArray.length - 1,
                false,
                false,
                false,
                IndexOptions.DOCS,
                DocValuesType.NONE,
                DocValuesSkipIndexType.NONE,
                -1,
                attributes,
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            return new FieldInfos(fieldInfosArray);
        }

        @Override
        public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
            throws IOException {
            delegate.write(directory, segmentInfo, segmentSuffix, infos, context);
        }
    }
}
