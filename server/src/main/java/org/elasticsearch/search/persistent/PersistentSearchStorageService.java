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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.PersistentSearchService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class PersistentSearchStorageService {
    public static final String INDEX = ".persistent_search_results";
    public static final String ID_FIELD = "id";
    public static final String RESPONSE_FIELD = "response";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";

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
            return jsonBuilder()
                .startObject()
                    .startObject(SINGLE_MAPPING_NAME)
                        .startObject("_meta")
                            .field("version", Version.CURRENT)
                        .endObject()
                        .field("dynamic", "strict")
                        .startObject("properties")
                            .startObject(ID_FIELD)
                                .field("type", "keyword")
                            .endObject()
                            .startObject(RESPONSE_FIELD)
                                .field("type", "binary")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + INDEX, e);
        }
    }

    public static List<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return List.of(SystemIndexDescriptor.builder()
            .setIndexPattern(INDEX)
            .setDescription("persistent search results")
            .setPrimaryIndex(INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setVersionMetaKey("version")
            .setOrigin("persistent_search")
            .build()
        );
    }

    private final Client client;
    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);

    public PersistentSearchStorageService(Client client) {
        this.client = client;
    }

    public void storeResult(PersistentSearchResponse persistentSearchResponse, ActionListener<Void> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(persistentSearchResponse.getId());

            logger.info("Storing {}", persistentSearchResponse.getSearchResponse());
            try (XContentBuilder builder = jsonBuilder()) {
                indexRequest.source(persistentSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
            logger.info("Response serialized");
            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                    logger.info("Error storing result", e);
                }
            });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void getPersistentSearchResult(String id, ActionListener<PersistentSearchResponse> listener) {
        final GetRequest getRequest = new GetRequest(INDEX).id(id);
        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isSourceEmpty()) {
                    listener.onFailure(new RuntimeException("Unable to find partial shard result with value"));
                    return;
                }

                try {
                    final PersistentSearchResponse persistentSearchResponse =
                        PersistentSearchResponse.fromXContent(getResponse.getSource());
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

    public SearchResponse getPartialResult(String id) {
        PlainActionFuture<PersistentSearchResponse> future = PlainActionFuture.newFuture();
        getPersistentSearchResult(id, future);

        try {
            return future.actionGet(5, TimeUnit.SECONDS).getSearchResponse();
        } catch (ElasticsearchTimeoutException e) {
            throw new RuntimeException("Unable to get partial search response with id " + id, e);
        }
    }

    private static boolean isExpectedCacheGetException(Exception e) {
        if (TransportActions.isShardNotAvailableException(e)
            || e instanceof ConnectTransportException
            || e instanceof ClusterBlockException) {
            return true;
        }
        final Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException || cause instanceof ConnectTransportException;
    }
}
