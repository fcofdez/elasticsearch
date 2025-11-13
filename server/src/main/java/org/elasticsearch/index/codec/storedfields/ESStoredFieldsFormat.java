/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.util.NamedSPILoader;

public abstract class ESStoredFieldsFormat extends StoredFieldsFormat implements NamedSPILoader.NamedSPI {
    private static final class Holder {
        private static final NamedSPILoader<ESStoredFieldsFormat> LOADER = new NamedSPILoader<>(ESStoredFieldsFormat.class);

        private Holder() {}

        static NamedSPILoader<ESStoredFieldsFormat> getLoader() {
            if (LOADER == null) {
                throw new IllegalStateException(
                    "You tried to lookup a DocValuesFormat by name before all formats could be initialized. "
                        + "This likely happens if you call DocValuesFormat#forName from a DocValuesFormat's ctor."
                );
            }
            return LOADER;
        }
    }

    public static ESStoredFieldsFormat forName(String name) {
        return Holder.getLoader().lookup(name);
    }
}
