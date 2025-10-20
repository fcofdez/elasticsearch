/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;

import java.io.IOException;

public class BloomFilterIdTerms extends Terms {
    private final Terms delegate;
    private final ES93BloomFilterStoredFieldsFormat.BloomFilterIdLookup bloomFilterLookup;

    private BloomFilterIdTerms(Terms delegate, ES93BloomFilterStoredFieldsFormat.BloomFilterIdLookup bloomFilterLookup) {
        this.delegate = delegate;
        this.bloomFilterLookup = bloomFilterLookup;
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new FilterTermsEnum(delegate.iterator()) {
            @Override
            public boolean seekExact(BytesRef text) throws IOException {
                if (bloomFilterLookup.mayContainTerm(text)) {
                    return super.seekExact(text);
                } else {
                    return false;
                }
            }
        };
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        return delegate.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
        return delegate.getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
        return delegate.getDocCount();
    }

    @Override
    public boolean hasFreqs() {
        return delegate.hasFreqs();
    }

    @Override
    public boolean hasOffsets() {
        return delegate.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
        return delegate.hasPositions();
    }

    @Override
    public boolean hasPayloads() {
        return delegate.hasPayloads();
    }

    static class FilterTermsEnum extends TermsEnum {
        private final TermsEnum delegate;

        FilterTermsEnum(TermsEnum delegate) {
            this.delegate = delegate;
        }

        @Override
        public AttributeSource attributes() {
            return delegate.attributes();
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            return delegate.seekExact(text);
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            return delegate.prepareSeekExact(text);
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return delegate.seekCeil(text);
        }

        @Override
        public void seekExact(long ord) throws IOException {
            delegate.seekExact(ord);
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            delegate.seekExact(term, state);
        }

        @Override
        public BytesRef term() throws IOException {
            return delegate.term();
        }

        @Override
        public long ord() throws IOException {
            return delegate.ord();
        }

        @Override
        public int docFreq() throws IOException {
            return delegate.docFreq();
        }

        @Override
        public long totalTermFreq() throws IOException {
            return delegate.totalTermFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return delegate.postings(reuse, flags);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return delegate.impacts(flags);
        }

        @Override
        public TermState termState() throws IOException {
            return delegate.termState();
        }

        @Override
        public BytesRef next() throws IOException {
            return delegate.next();
        }
    }

    static BloomFilterIdTerms from(Terms delegate, LeafReader reader) throws IOException {
        ES93BloomFilterStoredFieldsFormat.BloomFilterIdLookup bloomFilterIdLookup =
            new ES93BloomFilterStoredFieldsFormat.BloomFilterIdLookup(reader);
        return new BloomFilterIdTerms(delegate, bloomFilterIdLookup);
    }
}
