/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;

public class IdBloomFilterDelegatingStoredFieldsFormat extends StoredFieldsFormat {

    private final StoredFieldsFormat storedFieldsFormat;
    private final ES93BloomFilterStoredFieldsFormat bloomFilterFormat;

    public IdBloomFilterDelegatingStoredFieldsFormat(
        StoredFieldsFormat storedFieldsFormat,
        ES93BloomFilterStoredFieldsFormat bloomFilterFormat
    ) {
        this.storedFieldsFormat = storedFieldsFormat;
        this.bloomFilterFormat = bloomFilterFormat;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new FilterStoredFieldsReader(storedFieldsFormat.fieldsReader(directory, si, fn, context), bloomFilterFormat.fieldsReader(directory, si, fn, context));
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return new FilterStoredFieldsWriter(
            storedFieldsFormat.fieldsWriter(directory, si, context),
            bloomFilterFormat.fieldsWriter(directory, si, context)
        );
    }

    static class FilterStoredFieldsWriter extends StoredFieldsWriter {
        private final StoredFieldsWriter storedFieldsWriter;
        private final StoredFieldsWriter bloomFilterWriter;

        FilterStoredFieldsWriter(StoredFieldsWriter storedFieldsWriter, StoredFieldsWriter bloomFilterWriter) {
            this.storedFieldsWriter = storedFieldsWriter;
            this.bloomFilterWriter = bloomFilterWriter;
        }

        @Override
        public void startDocument() throws IOException {
            storedFieldsWriter.startDocument();
        }

        @Override
        public void finishDocument() throws IOException {
            storedFieldsWriter.finishDocument();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            storedFieldsWriter.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            storedFieldsWriter.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            storedFieldsWriter.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            storedFieldsWriter.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            if (info.getName().equals(IdFieldMapper.NAME)) {
                bloomFilterWriter.writeField(info, value);
            } else {
                storedFieldsWriter.writeField(info, value);
            }
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            storedFieldsWriter.writeField(info, value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            storedFieldsWriter.finish(numDocs);
            bloomFilterWriter.finish(numDocs);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(storedFieldsWriter, bloomFilterWriter);
        }

        @Override
        public long ramBytesUsed() {
            return storedFieldsWriter.ramBytesUsed() + bloomFilterWriter.ramBytesUsed();
        }
    }

    static class FilterStoredFieldsReader extends StoredFieldsReader {
        private final StoredFieldsReader storedFieldsReader;
        private final StoredFieldsReader bloomFilterStoredFieldsReader;

        FilterStoredFieldsReader(StoredFieldsReader storedFieldsReader, StoredFieldsReader bloomFilterStoredFieldsReader) {
            this.storedFieldsReader = storedFieldsReader;
            this.bloomFilterStoredFieldsReader = bloomFilterStoredFieldsReader;
        }

        @Override
        public StoredFieldsReader clone() {
            // TODO: fix this
            return this;
        }

        @Override
        public void checkIntegrity() throws IOException {
            storedFieldsReader.checkIntegrity();
            bloomFilterStoredFieldsReader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(storedFieldsReader, bloomFilterStoredFieldsReader);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            // This relies on the fact that the stored fields in the readers are disjoint
            // (i.e. the _id is only in the bloomFilterStoredFieldsReader)
            bloomFilterStoredFieldsReader.document(docID, visitor);
            storedFieldsReader.document(docID, visitor);
        }
    }
}
