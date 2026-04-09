/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Elassandra: public entry for constructing an embedded {@link Node} from {@code org.apache.cassandra}
 * where {@link Node}'s constructor is protected.
 */

package org.opensearch.node;

import org.opensearch.env.Environment;
import org.opensearch.plugins.Plugin;

import java.util.Collection;

/**
 * Embedded OpenSearch node used by {@link org.apache.cassandra.service.ElassandraDaemon}.
 */
public class ElassandraNode extends Node {

    public ElassandraNode(
        Environment environment,
        Collection<Class<? extends Plugin>> classpathPlugins,
        boolean forbidPrivateIndexSettings
    ) {
        super(environment, classpathPlugins, forbidPrivateIndexSettings);
    }
}
