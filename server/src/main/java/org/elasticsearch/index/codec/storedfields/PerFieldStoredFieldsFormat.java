/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Enables per field stored fields format support.
 *
 * <p> This class uses SPI to resolve format names.</p>
 *
 * <p> Files written by each stored fields format should use different file extensions, this is enforced during the writer creation.</p>
 */
public abstract class PerFieldStoredFieldsFormat extends StoredFieldsFormat {
    public static final String STORED_FIELD_FORMAT_ATTRIBUTE_KEY = "stored_field_format";

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new PerFieldStoredFieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return new PerFieldStoredFieldsWriter(directory, si, context);
    }

    protected abstract ESStoredFieldsFormat getStoredFieldsFormatForField(String field);

    class PerFieldStoredFieldsWriter extends StoredFieldsWriter {

        private final IntObjectHashMap<StoredFieldsWriter> fields = new IntObjectHashMap<>();
        private final Map<StoredFieldsFormat, StoredFieldsWriterAndExtensions> formatWriters = new HashMap<>();

        private final Directory directory;
        private final SegmentInfo si;
        private final IOContext context;

        private int numStartedDocs = 0;
        private int numFinishedDocs = 0;

        PerFieldStoredFieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
            this.directory = directory;
            this.si = si;
            this.context = context;
        }

        @Override
        public void startDocument() throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().startDocument();
            }
            numStartedDocs++;
        }

        @Override
        public void finishDocument() throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().finishDocument();
            }
            numFinishedDocs++;
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().finish(numDocs);
            }
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            Map<StoredFieldsWriter, Set<String>> writersToFields = new IdentityHashMap<>();
            for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
                StoredFieldsWriter writer = getWriterForField(mergeFieldInfo);
                var fieldsForWriter = writersToFields.computeIfAbsent(writer, ignored -> new HashSet<>());
                fieldsForWriter.add(mergeFieldInfo.name);
            }

            var totalDocs = 0;
            for (Map.Entry<StoredFieldsWriter, Set<String>> storedFieldsWriterToFields : writersToFields.entrySet()) {
                totalDocs += storedFieldsWriterToFields.getKey().merge(restrictFields(mergeState, storedFieldsWriterToFields.getValue()));
            }
            return totalDocs;
        }

        private MergeState restrictFields(MergeState in, Set<String> fields) {
            return new MergeState(
                in.docMaps,
                in.segmentInfo,
                in.mergeFieldInfos,
                in.storedFieldsReaders,
                in.termVectorsReaders,
                in.normsProducers,
                in.docValuesProducers,
                in.fieldInfos,
                in.liveDocs,
                in.fieldsProducers,
                in.pointsReaders,
                in.knnVectorsReaders,
                in.maxDocs,
                in.infoStream,
                in.intraMergeTaskExecutor,
                in.needsIndexSort
            );
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formatWriters.values());
        }

        @Override
        public long ramBytesUsed() {
            long ramBytesUsed = 0;
            for (var writer : formatWriters.values()) {
                ramBytesUsed += writer.writer().ramBytesUsed();
            }
            return ramBytesUsed;
        }

        private StoredFieldsWriter getWriterForField(FieldInfo field) throws IOException {
            var writer = fields.get(field.number);
            if (writer != null) {
                return writer;
            }

            var format = getStoredFieldsFormatForField(field.name);

            if (format == null) {
                throw new IllegalStateException("invalid null StoredFieldsFormat for field=\"" + field.name + "\"");
            }

            var formatWriter = formatWriters.get(format);
            if (formatWriter == null) {
                for (StoredFieldsWriterAndExtensions value : formatWriters.values()) {
                    if (Sets.intersection(value.extensions(), format.getFileExtensions()).isEmpty() == false) {
                        throw new IllegalStateException(
                            "File extension conflict for field '"
                                + field.name
                                + "': format "
                                + format.getName()
                                + " has overlapping extensions with existing format"
                        );
                    }
                }
                formatWriter = new StoredFieldsWriterAndExtensions(format.fieldsWriter(directory, si, context), format.getFileExtensions());

                // Ensure that the doc count is consistent so when #finish is called
                // all formats have a consistent doc count
                for (int i = 0; i < numStartedDocs; i++) {
                    formatWriter.writer().startDocument();
                }
                for (int i = 0; i < numFinishedDocs; i++) {
                    formatWriter.writer().startDocument();
                }

                var previous = formatWriters.put(format, formatWriter);
                assert previous == null;
            }
            fields.put(field.number, formatWriter.writer());
            field.putAttribute(STORED_FIELD_FORMAT_ATTRIBUTE_KEY, format.getName());

            return formatWriter.writer();
        }
    }

    record StoredFieldsWriterAndExtensions(StoredFieldsWriter writer, Set<String> extensions) implements Closeable {
        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    public static class PerFieldStoredFieldsReader extends StoredFieldsReader {
        private final Map<String, StoredFieldsReader> formatStoredFieldReaders;
        private final Map<String, StoredFieldsReader> perFieldStoredFieldReaders;

        PerFieldStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            HashMap<String, StoredFieldsReader> formatStoredFieldReaders = new HashMap<>();
            HashMap<String, StoredFieldsReader> perFieldStoredFieldReaders = new HashMap<>();
            boolean success = false;
            try {
                for (FieldInfo fi : fn) {
                    final String formatName = fi.getAttribute(STORED_FIELD_FORMAT_ATTRIBUTE_KEY);
                    if (formatName != null) {
                        var storedFieldsReader = formatStoredFieldReaders.get(formatName);
                        if (storedFieldsReader == null) {
                            ESStoredFieldsFormat format = ESStoredFieldsFormat.forName(formatName);
                            storedFieldsReader = format.fieldsReader(directory, si, fn, context);
                            var previous = formatStoredFieldReaders.put(formatName, storedFieldsReader);
                            assert previous == null;
                        }
                        perFieldStoredFieldReaders.put(fi.name, storedFieldsReader);
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(formatStoredFieldReaders.values());
                }
            }
            this.formatStoredFieldReaders = Collections.unmodifiableMap(formatStoredFieldReaders);
            this.perFieldStoredFieldReaders = Collections.unmodifiableMap(perFieldStoredFieldReaders);
        }

        PerFieldStoredFieldsReader(
            Map<String, StoredFieldsReader> formatStoredFieldReaders,
            Map<String, StoredFieldsReader> perFieldStoredFieldReaders
        ) {
            this.formatStoredFieldReaders = Collections.unmodifiableMap(formatStoredFieldReaders);
            this.perFieldStoredFieldReaders = Collections.unmodifiableMap(perFieldStoredFieldReaders);
        }

        @Override
        public StoredFieldsReader clone() {
            Map<String, StoredFieldsReader> clonedFormats = Maps.newMapWithExpectedSize(formatStoredFieldReaders.size());
            Map<String, StoredFieldsReader> clonedFields = Maps.newMapWithExpectedSize(perFieldStoredFieldReaders.size());
            for (Map.Entry<String, StoredFieldsReader> entry : formatStoredFieldReaders.entrySet()) {
                clonedFormats.put(entry.getKey(), entry.getValue().clone());
            }
            // TODO: fix this
            return new PerFieldStoredFieldsReader(clonedFormats, Map.of());
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            Map<String, StoredFieldsReader> mergeFormats = Maps.newMapWithExpectedSize(formatStoredFieldReaders.size());
            Map<String, StoredFieldsReader> clonedFields = Maps.newMapWithExpectedSize(perFieldStoredFieldReaders.size());
            for (Map.Entry<String, StoredFieldsReader> entry : formatStoredFieldReaders.entrySet()) {
                mergeFormats.put(entry.getKey(), entry.getValue().getMergeInstance());
            }
            // TODO: fix this
            return new PerFieldStoredFieldsReader(mergeFormats, Map.of());
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (StoredFieldsReader storedFieldsReader : formatStoredFieldReaders.values()) {
                storedFieldsReader.checkIntegrity();
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formatStoredFieldReaders.values());
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            for (StoredFieldsReader storedFieldsReader : formatStoredFieldReaders.values()) {
                storedFieldsReader.document(docID, visitor);
            }
        }

        @Nullable
        public StoredFieldsReader getReaderForField(String fieldName) {
            return perFieldStoredFieldReaders.get(fieldName);
        }
    }
}
