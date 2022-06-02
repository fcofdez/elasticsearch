/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DesiredNodeSerializationTests extends AbstractSerializingTestCase<DesiredNode> {
    @Override
    protected DesiredNode doParseInstance(XContentParser parser) throws IOException {
        return DesiredNode.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DesiredNode> instanceReader() {
        return DesiredNode::readFrom;
    }

    @Override
    protected DesiredNode createTestInstance() {
        return DesiredNodesTestCase.randomDesiredNodeWithRandomSettings();
    }

    @Override
    protected DesiredNode mutateInstance(DesiredNode instance) throws IOException {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors(),
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 1 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors() + 1,
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 2 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors() + 2,
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 3 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors() + 4,
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 4 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors() + 5,
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 5 -> new DesiredNode(
                instance.settings(),
                instance.minProcessors() + 6,
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            default -> throw new IllegalStateException("Unexpected");
        };
    }
}
