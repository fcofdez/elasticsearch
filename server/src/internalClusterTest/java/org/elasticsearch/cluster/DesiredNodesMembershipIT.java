/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesResponse;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.assertDesiredNodesMembershipIsCorrect;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.createDesiredNodes;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodeWithName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesMembershipIT extends ESIntegTestCase {
    public void testSimpleCase() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::createActualizedDesiredNode).toList();
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }

        final var newerDesiredNodes = createDesiredNodes(desiredNodes.historyID(), desiredNodes.version() + 1, desiredNodes.nodes());
        updateDesiredNodes(newerDesiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }
    }

    public void testIdempotentUpdateWithUpdatedMembership() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::createActualizedDesiredNode).toList();
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }
    }

    public void testMemberDesiredNodesAreKeptAsMemberEvenIfNodesLeavesTemporarily() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::createActualizedDesiredNode).toList();
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        updateDesiredNodes(desiredNodes);

        final var clusterState = client().admin().cluster().prepareState().get().getState();
        assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);

        final var leavingNodeNames = randomSubsetOf(nodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var newClusterState = client().admin().cluster().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(newClusterState);

        for (String leavingNodeName : leavingNodeNames) {
            final var desiredNode = latestDesiredNodes.find(leavingNodeName);
            assertThat(desiredNode.actualized(), is(equalTo(true)));
        }
    }

    public void testMembershipInformationIsClearedAfterHistoryIdChanges() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var clusterNodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = clusterNodeNames.stream().map(this::createActualizedDesiredNode).toList();
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        updateDesiredNodes(desiredNodes);

        final var clusterState = client().admin().cluster().prepareState().get().getState();
        assertDesiredNodesMembershipIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);

        // Stop some nodes, these shouldn't be members within the new desired node's history until they join back
        final var leavingNodeNames = randomSubsetOf(clusterNodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var newDesiredNodesHistory = new DesiredNodes(UUIDs.randomBase64UUID(random()), 1, desiredNodes.nodes());

        final var response = updateDesiredNodes(newDesiredNodesHistory);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(true)));

        final var updatedClusterState = client().admin().cluster().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(updatedClusterState);

        for (String clusterNodeName : clusterNodeNames) {
            final var desiredNode = latestDesiredNodes.find(clusterNodeName);
            assertThat(desiredNode.pending(), is(equalTo(leavingNodeNames.contains(clusterNodeName))));
        }
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(DesiredNodes desiredNodes) {
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            desiredNodes.historyID(),
            desiredNodes.version(),
            desiredNodes.nodes().stream().map(DesiredNodes.DesiredNodeWithStatus::desiredNode).toList()
        );
        return client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private DesiredNodes.DesiredNodeWithStatus createActualizedDesiredNode(String nodeName) {
        return new DesiredNodes.DesiredNodeWithStatus(randomDesiredNodeWithName(nodeName), DesiredNodes.Status.ACTUALIZED);
    }

    private DesiredNodes.DesiredNodeWithStatus createPendingDesiredNode() {
        return new DesiredNodes.DesiredNodeWithStatus(randomDesiredNodeWithName(randomAlphaOfLength(10)), DesiredNodes.Status.PENDING);
    }
}
