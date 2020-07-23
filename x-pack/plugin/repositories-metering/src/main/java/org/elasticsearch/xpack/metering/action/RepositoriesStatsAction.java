/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.metering.action;

import org.elasticsearch.action.ActionType;

public class RepositoriesStatsAction extends ActionType<RepositoriesNodesStatsResponse> {
    public static final RepositoriesStatsAction INSTANCE = new RepositoriesStatsAction();

    static final String NAME = "cluster:monitor/xpack/repositories_metering/request_counters";

    public RepositoriesStatsAction() {
        super(NAME, (in) -> null);
    }
}
