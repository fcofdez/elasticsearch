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

package org.elasticsearch.search.persistent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

public class PersistentSearchId  {
    private final String searchId;
    private final TaskId taskId;
    private final String encodedId;

    public PersistentSearchId(String searchId, TaskId taskId) {
        this(searchId, taskId, encode(searchId, taskId));
    }

    public PersistentSearchId(String searchId, TaskId taskId, String encodedId) {
        this.searchId = searchId;
        this.taskId = taskId;
        this.encodedId = encodedId;
    }

    public String getSearchId() {
        return searchId;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public String getEncodedId() {
        return encodedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentSearchId that = (PersistentSearchId) o;
        return Objects.equals(searchId, that.searchId) &&
            Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchId, taskId);
    }

    public static String encode(String docId, TaskId taskId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(docId);
            out.writeString(taskId.toString());
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static PersistentSearchId decode(String id) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        String docId;
        String taskId;
        try (StreamInput in = new ByteBufferStreamInput(byteBuffer)) {
            docId = in.readString();
            taskId = in.readString();
            if (in.available() > 0) {
                throw new IllegalArgumentException("invalid id: [" + id + "]");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        return new PersistentSearchId(docId, new TaskId(taskId), id);
    }
}
