/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class PersistentSearchShardId implements Comparable<PersistentSearchShardId>, Writeable {
    private final SearchShard searchShard;
    private final String searchId;
    private final int shardIndex;

    PersistentSearchShardId(SearchShard searchShard, String searchId, int shardIndex) {
        this.searchShard = searchShard;
        this.searchId = searchId;
        this.shardIndex = shardIndex;
    }

    PersistentSearchShardId(StreamInput in) throws IOException {
        this.searchShard = new SearchShard(in);
        this.searchId = in.readString();
        this.shardIndex = in.readInt();
    }

    public SearchShard getSearchShard() {
        return searchShard;
    }

    public String getSearchId() {
        return searchId;
    }

    public String getDocId() {
        return String.join("/", searchId, Integer.toString(shardIndex));
    }

    public int getShardIndex() {
        return shardIndex;
    }

    @Override
    public int compareTo(PersistentSearchShardId o) {
        return searchShard.compareTo(o.searchShard);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchShard.writeTo(out);
        out.writeString(searchId);
        out.writeInt(shardIndex);
    }
}
