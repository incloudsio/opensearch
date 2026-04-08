/*
 * Compile-only stub for OpenSearch side-car :server:compileJava.
 * Full ElassandraDaemon lives in this repo under org.apache.cassandra.service and targets Elasticsearch 6.8 bootstrap.
 */
package org.apache.cassandra.service;

import org.opensearch.node.Node;

/**
 * Satisfies references from org.elassandra.index.* until the daemon is ported to org.opensearch.bootstrap.
 */
public class ElassandraDaemon {

    public static ElassandraDaemon instance = new ElassandraDaemon();

    private ElassandraDaemon() {}

    public Node node() {
        return null;
    }
}
