/*
 * Compile stub for OpenSearch side-car (:server:compileJava and test-framework compile).
 * Full ElassandraDaemon lives in this repo under org.apache.cassandra.service and targets Elasticsearch 6.8 bootstrap.
 */
package org.apache.cassandra.service;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;

import java.util.Collection;

/**
 * Minimal API surface used by {@code org.opensearch.test.ESSingleNodeTestCase} after import rewrites; runtime tests still require a full daemon port.
 */
public class ElassandraDaemon {

    public static ElassandraDaemon instance = new ElassandraDaemon();

    private Node node;

    private ElassandraDaemon() {}

    public ElassandraDaemon(Environment env) {}

    public Node node() {
        return node;
    }

    public void node(Node newNode) {
        this.node = newNode;
    }

    public Settings nodeSettings(Settings settings) {
        return settings;
    }

    public Node newNode(
        Settings settings,
        Collection<Class<? extends Plugin>> classpathPlugins,
        boolean forbidPrivateIndexSettings
    ) {
        throw new UnsupportedOperationException("ElassandraDaemon OpenSearch stub: use full daemon port for tests");
    }

    public void activate(
        boolean tool,
        boolean daemonize,
        Settings settings,
        Environment environment,
        Collection<Class<? extends Plugin>> classpathPlugins
    ) {
        throw new UnsupportedOperationException("ElassandraDaemon OpenSearch stub: use full daemon port for tests");
    }

    public void ringReady() {}
}
