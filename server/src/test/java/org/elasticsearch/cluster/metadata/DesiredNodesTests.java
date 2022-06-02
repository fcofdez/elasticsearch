/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DesiredNodesTests extends DesiredNodesTestCase {

    public void testDuplicatedExternalIDsAreNotAllowed() {
        final String duplicatedExternalID = UUIDs.randomBase64UUID();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNodes(
                UUIDs.randomBase64UUID(),
                2,
                randomList(
                    2,
                    10,
                    () -> new DesiredNodes.DesiredNodeWithStatus(
                        randomDesiredNode(
                            Version.CURRENT,
                            (settings) -> settings.put(NODE_EXTERNAL_ID_SETTING.getKey(), duplicatedExternalID)
                        ),
                        randomFrom(DesiredNodes.Status.values())
                    )
                )
            )
        );
        assertThat(exception.getMessage(), containsString("Some nodes contain the same setting value"));
    }

    public void testPreviousVersionsWithSameHistoryIDAreSuperseded() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes previousDesiredNodes = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(previousDesiredNodes), is(equalTo(false)));
    }

    public void testIsSupersededByADifferentHistoryID() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes differentHistoryID = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(differentHistoryID), is(equalTo(true)));
    }

    public void testHasSameVersion() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes desiredNodesWithDifferentVersion = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        final DesiredNodes desiredNodesWithDifferentHistoryID = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            desiredNodes.version(),
            Collections.emptyList()
        );

        assertThat(desiredNodes.hasSameVersion(desiredNodes), is(equalTo(true)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentVersion), is(equalTo(false)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentHistoryID), is(equalTo(false)));
    }

    public void testClusterStateRemainsTheSameIfThereAreNoDesiredNodesDuringMembershipInfoUpgrade() {
        final var clusterState = ClusterState.builder(new ClusterName("test")).build();

        assertThat(DesiredNodes.updateDesiredNodesMembershipIfNeeded(clusterState), is(sameInstance(clusterState)));
    }

    public void testDesiredNodesMembershipIsUpdatedUsingCurrentClusterNodes() {
        final var actualizedDesiredNodes = randomList(0, 5, this::createActualizedDesiredNode);
        final var unknownDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);
        final var pendingDesiredNodes = randomList(1, 5, this::createPendingDesiredNode);

        final var discoveryNodes = DiscoveryNodes.builder();

        for (DesiredNodes.DesiredNodeWithStatus actualizedDesiredNode : actualizedDesiredNodes) {
            assertThat(actualizedDesiredNode.actualized(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(actualizedDesiredNode.externalId()));
        }

        for (DesiredNodes.DesiredNodeWithStatus pendingDesiredNode : pendingDesiredNodes) {
            assertThat(pendingDesiredNode.pending(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(pendingDesiredNode.externalId()));
        }

        // Add some nodes in the cluster that are not part of the desired nodes
        for (int i = 0; i < randomInt(5); i++) {
            discoveryNodes.add(newDiscoveryNode(UUIDs.randomBase64UUID(random())));
        }

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, unknownDesiredNodes, pendingDesiredNodes);

        final var clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes)).build())
            .build();

        final var updatedClusterState = DesiredNodes.updateDesiredNodesMembershipIfNeeded(clusterState);
        assertThat(updatedClusterState, is(not(sameInstance(clusterState))));

//        assertDesiredNodesMembershipIsCorrect(
//            updatedClusterState,
//            Stream.concat(actualizedDesiredNodes.stream(), pendingDesiredNodes.stream()).toList(),
//            unknownDesiredNodes
//        );
    }

    public void testClusterStateIsNotChangedWhenDesiredNodesMembershipDoNotChange() {
        final var actualizedDesiredNodes = randomList(1, 5, this::createActualizedDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var discoveryNodes = DiscoveryNodes.builder();
        for (DesiredNodes.DesiredNodeWithStatus actualizedDesiredNode : actualizedDesiredNodes) {
            assertThat(actualizedDesiredNode.actualized(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(actualizedDesiredNode.externalId()));
        }

        // Add some nodes in the cluster that are not part of the desired nodes
        for (int i = 0; i < randomInt(5); i++) {
            discoveryNodes.add(newDiscoveryNode(UUIDs.randomBase64UUID(random())));
        }

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        final var clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes)).build())
            .build();

        final var updatedClusterState = DesiredNodes.updateDesiredNodesMembershipIfNeeded(clusterState);
        assertThat(updatedClusterState, is(sameInstance(clusterState)));
    }

    public void testNewDesiredNodesAreStoredInClusterStateIfTheyChange() {
        final var metadata = Metadata.builder();

        DesiredNodes previousDesiredNodes;
        final boolean desiredNodesInClusterState = randomBoolean();
        if (desiredNodesInClusterState) {
            final var actualizedDesiredNodes = randomList(1, 5, this::createActualizedDesiredNode);
            final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

            previousDesiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

            metadata.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(previousDesiredNodes));
        }

        final var clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata.build()).build();

        final var newDesiredNodes = createDesiredNodes(
            // Reuse the same nodes from the previous desired nodes or create new ones
            desiredNodesInClusterState && randomBoolean()
                ? List.of() //previousDesiredNodes.nodes().stream().map(DesiredNode::withUnknownMembershipStatus).toList()
                : randomList(1, 10, this::createPendingDesiredNode)
        );

        final var updatedClusterState = DesiredNodes.updateDesiredNodesMembershipIfNeeded(clusterState, newDesiredNodes);

        assertThat(updatedClusterState, is(not(sameInstance(clusterState))));
        assertThat(DesiredNodes.latestFromClusterState(updatedClusterState), is(equalTo(newDesiredNodes)));
        for (DesiredNodes.DesiredNodeWithStatus desiredNode : DesiredNodes.latestFromClusterState(updatedClusterState)) {
            assertThat(desiredNode.actualized(), is(equalTo(false)));
        }
    }

//    public void testMembershipInformationIsCarriedOverInNewVersions() {
//        final var knownDesiredNodes = randomList(1, 5, this::createMemberDesiredNode);
//        final var unknownDesiredNodes = randomList(0, 5, this::createDesiredNode);
//
//        final var desiredNodes = createDesiredNodes(knownDesiredNodes, unknownDesiredNodes);
//
//        final var newKnownDesiredNodes = randomSubsetOf(desiredNodes.nodes()).stream().map(desiredNode -> {
//            if (randomBoolean()) {
//                return desiredNodeWithDifferentSpecsAndSameExternalId(desiredNode);
//            } else {
//                return desiredNode.withUnknownMembershipStatus();
//            }
//        }).toList();
//        final List<DesiredNode> newUnknownDesiredNodes = randomList(newKnownDesiredNodes.isEmpty() ? 1 : 0, 5, this::createDesiredNode);
//
//        final boolean withNewHistoryId = randomBoolean();
//        final var newDesiredNodes = createDesiredNodes(
//            withNewHistoryId ? UUIDs.randomBase64UUID(random()) : desiredNodes.historyID(),
//            desiredNodes.version() + 1,
//            newKnownDesiredNodes,
//            newUnknownDesiredNodes
//        );
//
//        final var newDesiredNodesWithMembershipInformation = newDesiredNodes.withMembershipInformationFrom(desiredNodes);
//
//        if (withNewHistoryId) {
//            for (DesiredNode desiredNode : newDesiredNodesWithMembershipInformation) {
//                assertThat(desiredNode.isMember(), is(equalTo(false)));
//            }
//        } else {
//            assertDesiredNodesMembershipIsCorrect(
//                newDesiredNodesWithMembershipInformation,
//                newKnownDesiredNodes,
//                Stream.concat(unknownDesiredNodes.stream(), newUnknownDesiredNodes.stream()).toList()
//            );
//        }
//    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
    }

    private DesiredNodes.DesiredNodeWithStatus createActualizedDesiredNode() {
        return new DesiredNodes.DesiredNodeWithStatus(
            randomDesiredNodeWithExternalId(UUIDs.randomBase64UUID(random())),
            DesiredNodes.Status.ACTUALIZED
        );
    }

    private DesiredNodes.DesiredNodeWithStatus createPendingDesiredNode() {
        return new DesiredNodes.DesiredNodeWithStatus(
            randomDesiredNodeWithExternalId(UUIDs.randomBase64UUID(random())),
            DesiredNodes.Status.PENDING
        );
    }

    private DesiredNode desiredNodeWithDifferentSpecsAndSameExternalId(DesiredNode desiredNode) {
        return new DesiredNode(
            desiredNode.settings(),
            desiredNode.minProcessors() + randomIntBetween(1, 10),
            ByteSizeValue.ofGb(desiredNode.memory().getGb() + randomIntBetween(15, 20)),
            ByteSizeValue.ofGb(desiredNode.storage().getGb() + randomIntBetween(1, 100)),
            Version.CURRENT
        );
    }
}
