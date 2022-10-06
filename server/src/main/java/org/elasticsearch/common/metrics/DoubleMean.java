/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class DoubleMean implements Writeable {
    public static final DoubleMean ZERO = new DoubleMean(0, 0);
    private final double sum;
    private final long count;

    DoubleMean(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public DoubleMean(StreamInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    public double mean() {
        return count == 0 ? 0 : sum / count;
    }

    public DoubleMean add(DoubleMean other) {
        return new DoubleMean(sum + other.sum, Math.addExact(count, other.count));
    }
}
