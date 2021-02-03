/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionType;

public class ExecutePersistentQueryFetchAction extends ActionType<ExecutePersistentQueryFetchResponse> {
    public static String NAME = "indices:data/read/persistent_search[phase/query+fetch]";
    public static ExecutePersistentQueryFetchAction INSTANCE = new ExecutePersistentQueryFetchAction();

    public ExecutePersistentQueryFetchAction() {
        super(NAME, ExecutePersistentQueryFetchResponse::new);
    }
}
