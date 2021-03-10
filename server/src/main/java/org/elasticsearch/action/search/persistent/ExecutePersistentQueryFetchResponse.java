/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ExecutePersistentQueryFetchResponse extends ActionResponse {
    private final String docId;
    private final String nodeId;

    public ExecutePersistentQueryFetchResponse(String docId, String nodeId) {
        this.docId = docId;
        this.nodeId = nodeId;
    }

    public ExecutePersistentQueryFetchResponse(StreamInput in) throws IOException {
        super(in);
        this.docId = in.readString();
        this.nodeId = in.readString();
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeString(nodeId);
    }
}