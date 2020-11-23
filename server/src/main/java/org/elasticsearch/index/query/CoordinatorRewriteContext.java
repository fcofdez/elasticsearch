/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class CoordinatorRewriteContext extends QueryRewriteContext {

    static class ConstantField {
        private final String fieldName;
        private final long minTimestamp;
        private final long maxTimestamp;
        private final MappedFieldType fieldType;
        private final PointValues pointValues;

        public ConstantField(String fieldName, long minTimestamp, long maxTimestamp, MappedFieldType fieldType) {
            this.fieldName = fieldName;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.fieldType = fieldType;
            this.pointValues = new PointValues() {
                @Override
                public void intersect(IntersectVisitor visitor) {
                }

                @Override
                public long estimatePointCount(IntersectVisitor visitor) {
                    return 1;
                }

                @Override
                public byte[] getMinPackedValue() {
                    final byte[] encodedMin = new byte[Long.BYTES];
                    LongPoint.encodeDimension(minTimestamp, encodedMin, 0);
                    return encodedMin;
                }

                @Override
                public byte[] getMaxPackedValue() {
                    final byte[] encodedLong = new byte[Long.BYTES];
                    LongPoint.encodeDimension(maxTimestamp, encodedLong, 0);
                    return encodedLong;
                }

                @Override
                public int getNumDimensions() {
                    return 1;
                }

                @Override
                public int getNumIndexDimensions() {
                    return 1;
                }

                @Override
                public int getBytesPerDimension() {
                    return Long.BYTES;
                }

                @Override
                public long size() {
                    return 1;
                }

                @Override
                public int getDocCount() {
                    return 0;
                }
            };
        }

        public PointValues getPointValues() {
            return pointValues;
        }
    }

    private final Map<String, ConstantField> constantFields;

    public CoordinatorRewriteContext(NamedXContentRegistry xContentRegistry,
                                     NamedWriteableRegistry writeableRegistry,
                                     Client client,
                                     LongSupplier nowInMillis,
                                     List<ConstantField> fields) {
        super(xContentRegistry, writeableRegistry, client, nowInMillis);
        Map<String, ConstantField> constantFields = new HashMap<>(fields.size());
        for (ConstantField field : fields) {
            constantFields.put(field.fieldName, field);
        }
        this.constantFields = Collections.unmodifiableMap(constantFields);
    }

    IndexReader getIndexReader() {
        return new ConstantIndexReader();
    }

    public MappedFieldType getFieldType(String fieldName) {
        ConstantField constantField = constantFields.get(fieldName);
        if (constantField == null) {
            return null;
        }

        return constantField.fieldType;
    }

    class ConstantIndexReader extends LeafReader {
        ConstantIndexReader() {
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Terms terms(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NumericDocValues getNormValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldInfos getFieldInfos() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getLiveDocs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getPointValues(String field) {
            ConstantField constantField = constantFields.get(field);

            if (constantField == null) {
                return null;
            }

            return constantField.getPointValues();
        }

        @Override
        public void checkIntegrity() {
        }

        @Override
        public LeafMetaData getMetaData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Fields getTermVectors(int docID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numDocs() {
            return 0;
        }

        @Override
        public int maxDoc() {
            return 0;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() {

        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }
    }
}
