/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadsampler;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

public class DataStreamsWriteLoadSamplerTests extends ESTestCase {
    public void testOnlyDataStreamsWriteLoadIsSampled() {
        final var clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder().build()).build();
        final var dataStreamsWriteLoadSampler = new DataStreamsWriteLoadSampler(() -> clusterState, () -> 0L);
        dataStreamsWriteLoadSampler.sampleWriteLoadStats();
        fail();
    }
}
