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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.persistent.GetPersistentSearchAction;
import org.elasticsearch.action.search.persistent.GetPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchAction;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.core.IsEqual.equalTo;

public class PersistentSearchIT extends ESIntegTestCase {
    public void testBasicPersistentSearch() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName));
        PutMappingRequest request = new PutMappingRequest().indices(indexName).source("key", "type=keyword");
        client().admin().indices().putMapping(request).actionGet();
        ensureGreen(indexName);

        int docCount = randomIntBetween(50, 300);
        populateIndex(indexName, docCount);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        searchRequest.indices(indexName);
        searchRequest.source(SearchSourceBuilder.searchSource().aggregation(new TermsAggregationBuilder("agg").field("key")));

        boolean withoutHits = randomBoolean();
        if (withoutHits) {
            searchRequest.source().size(0);
        }

        final SubmitPersistentSearchResponse submitPersistentSearchResponse =
            client().execute(SubmitPersistentSearchAction.INSTANCE, searchRequest).actionGet();

        final GetPersistentSearchRequest getReq =
            new GetPersistentSearchRequest(submitPersistentSearchResponse.getSearchId().getSearchId());

        Thread.sleep(3_000);
        assertBusy(() -> {
            final PersistentSearchResponse persistentSearchResponse =
                client().execute(GetPersistentSearchAction.INSTANCE, getReq).actionGet();

            assertNotNull(persistentSearchResponse);
            if (withoutHits == false) {
                final SearchHit[] hits = persistentSearchResponse.getSearchResponse().getHits().getHits();
                assertThat(hits.length, equalTo(10));
            }

            final StringTerms agg = persistentSearchResponse.getSearchResponse().getAggregations().get("agg");
            assertThat(agg.getBucketByKey("bar").getDocCount(), equalTo((long) docCount));
        });
    }

    private void populateIndex(String indexName, int docCount) throws InterruptedException {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final String key = "key";
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource(key, "bar", "hola", randomAlphaOfLength(4)));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
    }
}
