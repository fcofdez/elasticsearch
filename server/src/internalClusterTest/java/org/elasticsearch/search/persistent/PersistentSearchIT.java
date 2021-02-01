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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchAction;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class PersistentSearchIT extends ESIntegTestCase {
    public void testBasicPersistentSearch() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName));
        populateIndex(indexName, 50);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        PlainActionFuture<SubmitPersistentSearchResponse> future = PlainActionFuture.newFuture();
        client().execute(SubmitPersistentSearchAction.INSTANCE, searchRequest, future);
        final SubmitPersistentSearchResponse submitPersistentSearchResponse = future.actionGet();
        Thread.sleep(40_000);
        //assertThat(submitPersistentSearchResponse.getSearchId().getEncodedId(), equalTo("Asd"));
    }

    private void populateIndex(String indexName, int docCount) throws InterruptedException {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final String key = "key";
        for (int i = docCount; i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource(key, "bar"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
    }
}
