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

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.PersistentSearchService;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportExecuteQueryFetchAction extends HandledTransportAction<ExecutePersistentQueryFetchRequest,
                                                                             ExecutePersistentQueryFetchResponse> {

    private final PersistentSearchService persistentSearchService;

    @Inject
    public TransportExecuteQueryFetchAction(String actionName,
                                            TransportService transportService,
                                            ActionFilters actionFilters,
                                            Writeable.Reader<ExecutePersistentQueryFetchRequest> reader,
                                            PersistentSearchService persistentSearchService) {
        super(actionName, transportService, actionFilters, reader);
        this.persistentSearchService = persistentSearchService;
    }

    @Override
    protected void doExecute(Task task,
                             ExecutePersistentQueryFetchRequest request,
                             ActionListener<ExecutePersistentQueryFetchResponse> listener) {
        persistentSearchService.executeAsyncQueryPhase(request, (SearchShardTask) task, listener);
    }
}
