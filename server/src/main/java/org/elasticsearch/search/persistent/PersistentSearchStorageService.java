/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.PersistentSearchService;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public class PersistentSearchStorageService {
    public static final String INDEX = ".persistent_search_results";
    public static final String ID_FIELD = "id";
    public static final String SEARCH_ID_FIELD = "search_id";
    public static final String RESPONSE_FIELD = "response";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String REDUCED_SHARDS_INDEX_FIELD = "reduced_shards_index_field";

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
                            .startObject(SEARCH_ID_FIELD)
                                .field("type", "keyword")
                            .endObject()
                            .startObject(RESPONSE_FIELD)
                                .field("type", "binary")
                            .endObject()
                            .startObject(REDUCED_SHARDS_INDEX_FIELD)
                                .field("type", "long")
                            .endObject()
                            .startObject(EXPIRATION_TIME_FIELD)
                                .field("type", "long")
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
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Logger logger = LogManager.getLogger(PersistentSearchService.class);

    public PersistentSearchStorageService(Client client, NamedWriteableRegistry namedWriteableRegistry) {
        this.client = client;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public void storeResult(PersistentSearchResponse persistentSearchResponse, ActionListener<String> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(persistentSearchResponse.getId());

            try (XContentBuilder builder = jsonBuilder()) {
                indexRequest.source(persistentSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));
            }
            client.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(persistentSearchResponse.getId());
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

    public void getPersistentSearchResponseAsync(String id, ActionListener<PersistentSearchResponse> listener) {
        final GetRequest getRequest = new GetRequest(INDEX).id(id);
        client.get(getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isSourceEmpty()) {
                    listener.onResponse(null);
                    return;
                }

                try {
                    final PersistentSearchResponse persistentSearchResponse =
                        PersistentSearchResponse.fromXContent(getResponse.getSource(), getResponse.getVersion(), namedWriteableRegistry);
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

    public PersistentSearchResponse getPersistentSearchResponse(String id) {
        PlainActionFuture<PersistentSearchResponse> future = PlainActionFuture.newFuture();
        getPersistentSearchResponseAsync(id, future);

        try {
            return future.actionGet(5, TimeUnit.SECONDS);
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

    public void deletePersistentSearchResults(List<String> persistentSearchResultIds, ActionListener<Collection<DeleteResponse>> listener) {
        // TODO: is there a more efficient way?
        GroupedActionListener<DeleteResponse> groupedListener = new GroupedActionListener<>(listener, persistentSearchResultIds.size());
        for (String persistentSearchResultId : persistentSearchResultIds) {
            final DeleteRequest deleteRequest = client.prepareDelete(INDEX, persistentSearchResultId).request();
            client.delete(deleteRequest, groupedListener);
        }
    }
}
