/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.DoubleMean;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class WriteLoadStats implements Writeable, ToXContentFragment {
    private final DoubleMean writeLoadMean;

    public WriteLoadStats() {
        this(DoubleMean.ZERO);
    }

    public WriteLoadStats(DoubleMean writeLoadMean) {
        this.writeLoadMean = writeLoadMean;
    }

    public WriteLoadStats(StreamInput in) throws IOException {
        this.writeLoadMean = new DoubleMean(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeLoadMean.writeTo(out);
    }

    public double indexingLoadAvg() {
        return writeLoadMean.mean();
    }

    public WriteLoadStats add(WriteLoadStats other) {
        if (other == null) {
            return this;
        }
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
