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

package org.elasticsearch.search;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.PersistentSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class PersistentSearchStorageService {
    public static final String INDEX = ".persistent_search_results";
    public static final String HEADERS_FIELD = "headers";
    public static final String RESPONSE_HEADERS_FIELD = "response_headers";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String RESULT_FIELD = "result";

    // Usually the settings, mappings and system index descriptor below
    // would be co-located with the SystemIndexPlugin implementation,
    // however in this case this service is in a different project to
    // AsyncResultsIndexPlugin, as are tests that need access to
    // #settings().

    static Settings settings() {
        return Settings.builder()
            .put("index.codec", "best_compression")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder()
                .startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject(HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESPONSE_HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESULT_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + INDEX, e);
        }
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(INDEX)
            .setDescription("Async search results")
            .setPrimaryIndex(INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setVersionMetaKey("version")
            .setOrigin("persistent_async_search")
            .build();
    }

    private final Client client;
    private final String index;

    public PersistentSearchStorageService(Client client, String index) {
        this.client = client;
        this.index = index;
    }

    public void storeResult(PersistentSearchResponse persistentSearchResponse, ActionListener<Void> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(index)
                .id(persistentSearchResponse.generateId());

            try (XContentBuilder builder = jsonBuilder()) {
                indexRequest.source(persistentSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void getPersistentSearchResult(String id, ActionListener<PersistentSearchResponse> listener) {
        final GetRequest getRequest = new GetRequest(index).id(id);
        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isSourceEmpty()) {
                    listener.onFailure(new RuntimeException("Unable to find partial shard result with value"));
                    return;
                }

                try {
                    final PersistentSearchResponse persistentSearchResponse = PersistentSearchResponse.fromXContent(getResponse.getSource());
                    listener.onResponse(persistentSearchResponse);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public void getTotalPersistentSearchResult(String searchId, ActionListener<SearchResponse> listener) {
        final GetRequest getRequest = new GetRequest(index).id(searchId);
        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isSourceEmpty()) {
                    listener.onFailure(new RuntimeException("Unable to find search with ID " + searchId));
                    return;
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public SearchResponse getPartialResult(String id) {
        PlainActionFuture<PersistentSearchResponse> future = PlainActionFuture.newFuture();
        getPersistentSearchResult(id, future);

        try {
            return future.actionGet(5, TimeUnit.SECONDS).getSearchResponse();
        } catch (ElasticsearchTimeoutException e) {
            throw new RuntimeException("Unable to get partial search response with id " + id, e);
        }
    }
}
