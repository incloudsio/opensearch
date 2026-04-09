/*
 * Elassandra bootstrap entry (OpenSearch side-car). The full implementation lives in the Elassandra fork
 * (CassandraDaemon + OpenSearch Node); this file keeps the public API used by tests and org.elassandra.*
 * while the JVM port is completed.
 */
package org.apache.cassandra.service;

import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;

import java.util.Collection;

/**
 * Singleton holder for the embedded OpenSearch {@link Node}, parallel to the Elasticsearch 6.8 Elassandra line.
 */
public class ElassandraDaemon {

    public static ElassandraDaemon instance = null;

    protected volatile Node node = null;
    protected org.opensearch.env.Environment env;

    public ElassandraDaemon(org.opensearch.env.Environment env) {
        this.env = env;
        instance = this;
    }

    public org.opensearch.env.Environment getEnvironment() {
        return env;
    }

    public Node node() {
        return node;
    }

    public Node node(Node node) {
        this.node = node;
        return node;
    }

    /**
     * Mirrors the fork signature; full Cassandra bootstrap + OpenSearch wiring is still being ported.
     */
    public void activate(
        boolean addShutdownHook,
        boolean createNode,
        Settings settings,
        org.opensearch.env.Environment activateEnv,
        Collection<Class<? extends Plugin>> classpathPlugins
    ) {
        throw new UnsupportedOperationException(
            "ElassandraDaemon.activate() is not wired in the OpenSearch side-car stub; run integration tests from the Elassandra 6.8 tree or complete the daemon port."
        );
    }

    public Settings nodeSettings(Settings settings) {
        return settings;
    }

    public Node newNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins, boolean forbidPrivateIndexSettings) {
        throw new UnsupportedOperationException(
            "ElassandraDaemon.newNode() is not wired in the OpenSearch side-car stub; complete the daemon port for runtime tests."
        );
    }

    public void ringReady() {
        // Overridden by tests that wrap the daemon
    }
}
