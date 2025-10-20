/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ES93BloomFilterStoredFieldsFormat extends StoredFieldsFormat {
    public static final String STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME = "ES93BloomFilterStoredFieldsFormat";
    public static final String STORED_FIELDS_BLOOM_FILTER_EXTENSION = "sfbf";
    public static final String STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION = "sfbfmd";
    private static final int VERSION_START = 1;
    private static final int VERSION_CURRENT = VERSION_START;
    private static final int NUM_HASH_FUNCTIONS = 7;

    // TODO: Make this dynamic
    private static final int DEFAULT_BLOOM_FILTER_SIZE = Math.toIntExact(ByteSizeValue.ofKb(128).getBytes());

    private final BigArrays bigArrays;
    private final String segmentSuffix;

    public ES93BloomFilterStoredFieldsFormat(BigArrays bigArrays, String segmentSuffix) {
        this.bigArrays = bigArrays;
        this.segmentSuffix = segmentSuffix;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new BloomFilterStoredFieldsReader(directory, si, fn, context, segmentSuffix);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return new BloomFilterStoredFieldsWriter(directory, si, context, segmentSuffix, bigArrays);
    }

    static class BloomFilterStoredFieldsWriter extends StoredFieldsWriter {
        private final IndexOutput bloomFilterOut;
        private final IndexOutput metadataOut;
        private final ByteArray buffer;
        private final List<Closeable> toClose = new ArrayList<>();
        private final int[] hashes;
        private volatile FieldInfo idFieldInfo;

        BloomFilterStoredFieldsWriter(
            Directory directory,
            SegmentInfo segmentInfo,
            IOContext context,
            String segmentSuffix,
            BigArrays bigArrays
        ) throws IOException {
            boolean success = false;
            try {
                bloomFilterOut = directory.createOutput(bloomFilterFileName(segmentInfo, segmentSuffix), context);
                toClose.add(bloomFilterOut);
                CodecUtil.writeIndexHeader(
                    bloomFilterOut,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_CURRENT,
                    segmentInfo.getId(),
                    segmentSuffix
                );

                metadataOut = directory.createOutput(bloomFilterMetadataFileName(segmentInfo, segmentSuffix), context);
                toClose.add(metadataOut);
                CodecUtil.writeIndexHeader(
                    metadataOut,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_CURRENT,
                    segmentInfo.getId(),
                    segmentSuffix
                );

                buffer = bigArrays.newByteArray(DEFAULT_BLOOM_FILTER_SIZE, false);
                toClose.add(buffer);

                hashes = new int[NUM_HASH_FUNCTIONS];
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        @Override
        public void startDocument() throws IOException {}

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            assert info.getName().equals(IdFieldMapper.NAME);
            idFieldInfo = info;
            for (int hash : hashTerm(value, hashes)) {
                hash = hash % DEFAULT_BLOOM_FILTER_SIZE;
                final int pos = hash >> 3;
                final int mask = 1 << (hash & 7);
                final byte val = (byte) (buffer.get(pos) | mask);
                buffer.set(pos, val);
            }
        }

        @Override
        public void finish(int numDocs) throws IOException {
            if (idFieldInfo == null) {
                return;
            }
            if (buffer.hasArray()) {
                bloomFilterOut.writeBytes(buffer.array(), 0, DEFAULT_BLOOM_FILTER_SIZE);
            } else {
                BytesReference.fromByteArray(buffer, DEFAULT_BLOOM_FILTER_SIZE).writeTo(new IndexOutputOutputStream(bloomFilterOut));
            }
            CodecUtil.writeFooter(bloomFilterOut);

            var bloomFilter = new BloomFilterMetadata(idFieldInfo, 0, bloomFilterOut.getFilePointer());
            bloomFilter.writeTo(metadataOut);
            metadataOut.writeVLong(bloomFilterOut.getFilePointer()); // Store the total bloom filter size
            CodecUtil.writeFooter(metadataOut);
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            return super.merge(mergeState);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(toClose);
        }

        @Override
        public long ramBytesUsed() {
            return buffer.ramBytesUsed();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            throwUnsupported();
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            throwUnsupported();
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            throwUnsupported();
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            throwUnsupported();
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            throwUnsupported();
        }

        private void throwUnsupported() {
            throw new UnsupportedOperationException("This format is only used for BloomFilter stored field " + IdFieldMapper.NAME);
        }
    }

    static class BloomFilterStoredFieldsReader extends StoredFieldsReader {
        private final FieldInfo idFieldInfo;
        private final BloomFilterMetadata bloomFilterMetadata;
        private final IndexInput bloomFilterIn;

        BloomFilterStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context, String segmentSuffix)
            throws IOException {
            List<Closeable> toClose = new ArrayList<>();
            var success = false;
            try (var metaInput = directory.openChecksumInput(bloomFilterMetadataFileName(si, segmentSuffix))) {
                CodecUtil.checkIndexHeader(
                    metaInput,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );
                bloomFilterMetadata = BloomFilterMetadata.readFrom(metaInput, fn);
                var totalBloomFilterSize = metaInput.readVLong();
                idFieldInfo = bloomFilterMetadata.fieldInfo;
                CodecUtil.checkFooter(metaInput);

                bloomFilterIn = directory.openInput(bloomFilterFileName(si, segmentSuffix), context);
                toClose.add(bloomFilterIn);
                CodecUtil.checkIndexHeader(
                    bloomFilterIn,
                    STORED_FIELDS_BLOOM_FILTER_FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );
                CodecUtil.retrieveChecksum(bloomFilterIn, totalBloomFilterSize);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        @Override
        public StoredFieldsReader clone() {
            return this;
        }

        @Override
        public void checkIntegrity() throws IOException {
            // TODO
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(bloomFilterIn);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            if (visitor.needsField(idFieldInfo) == StoredFieldVisitor.Status.YES) {
                visitor.binaryField(
                    idFieldInfo,
                    new StoredFieldDataInput(bloomFilterIn, Math.toIntExact(bloomFilterMetadata.bloomFilterSize))
                );
            }
        }
    }

    private static String bloomFilterMetadataFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
    }

    private static String bloomFilterFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_BLOOM_FILTER_EXTENSION);
    }

    // Uses MurmurHash3-128 to generate a 64-bit hash value, then picks 7 subsets of 31 bits each and returns the values in the
    // outputs array. This provides us with 7 reasonably independent hashes of the data for the cost of one MurmurHash3 calculation.
    static int[] hashTerm(BytesRef br, int[] outputs) {
        final long hash64 = ES87BloomFilterPostingsFormat.MurmurHash3.hash64(br.bytes, br.offset, br.length);
        final int upperHalf = (int) (hash64 >> 32);
        final int lowerHalf = (int) hash64;
        // Derive 7 hash outputs by combining the two 64-bit halves, adding the upper half multiplied with different small constants
        // without common gcd.
        outputs[0] = (lowerHalf + 2 * upperHalf) & 0x7FFF_FFFF;
        outputs[1] = (lowerHalf + 3 * upperHalf) & 0x7FFF_FFFF;
        outputs[2] = (lowerHalf + 5 * upperHalf) & 0x7FFF_FFFF;
        outputs[3] = (lowerHalf + 7 * upperHalf) & 0x7FFF_FFFF;
        outputs[4] = (lowerHalf + 11 * upperHalf) & 0x7FFF_FFFF;
        outputs[5] = (lowerHalf + 13 * upperHalf) & 0x7FFF_FFFF;
        outputs[6] = (lowerHalf + 17 * upperHalf) & 0x7FFF_FFFF;
        return outputs;
    }

    record BloomFilterMetadata(FieldInfo fieldInfo, long offset, long bloomFilterSize) {
        void writeTo(IndexOutput indexOut) throws IOException {
            indexOut.writeVInt(fieldInfo.number);
            indexOut.writeVLong(offset);
            indexOut.writeVLong(bloomFilterSize);
        }

        static BloomFilterMetadata readFrom(IndexInput in, FieldInfos fieldInfos) throws IOException {
            final var fieldInfo = fieldInfos.fieldInfo(in.readVInt());
            final long offset = in.readVLong();
            final int bloomFilterSize = in.readVInt();
            return new BloomFilterMetadata(fieldInfo, offset, bloomFilterSize);
        }
    }

    public static class BloomFilterIdLookup {
        private final RandomAccessInput bloomFilterIn;
        private final int bloomFilterSize;
        private final int[] hashes = new int[NUM_HASH_FUNCTIONS];

        public BloomFilterIdLookup(LeafReader reader) throws IOException {
            this(reader.storedFields());
        }

        public BloomFilterIdLookup(StoredFields storedFields) throws IOException {
            var dataInputRef = new AtomicReference<StoredFieldDataInput>();
            // TODO: handle lifecycle of this input
            storedFields.document(0, new StoredFieldVisitor() {
                @Override
                public Status needsField(FieldInfo fieldInfo) {
                    return fieldInfo.getName().equals(IdFieldMapper.NAME) ? Status.YES : Status.NO;
                }

                @Override
                public void binaryField(FieldInfo fieldInfo, StoredFieldDataInput value) {
                    assert fieldInfo.getName().equals(IdFieldMapper.NAME);
                    dataInputRef.set(value);
                }
            });
            // TODO: is there a workaround for this?
            this.bloomFilterSize = dataInputRef.get().getLength();
            this.bloomFilterIn = ((IndexInput) (dataInputRef.get().in())).randomAccessSlice(0, bloomFilterSize);
            assert bloomFilterIn != null;
        }

        public boolean mayContainTerm(BytesRef term) throws IOException {
            hashTerm(term, hashes);
            for (int hash : hashes) {
                hash = hash % bloomFilterSize;
                final int pos = hash >> 3;
                final int mask = 1 << (hash & 7);
                final byte bits = bloomFilterIn.readByte(pos);
                if ((bits & mask) == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
