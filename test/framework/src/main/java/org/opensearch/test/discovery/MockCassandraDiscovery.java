/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Elassandra: mock Cassandra discovery for tests (ported from Elasticsearch 6.8 fork).
 * OpenSearch 1.3 removed {@code DiscoveryPlugin#getDiscoveryTypes}; {@link org.elassandra.discovery.CassandraDiscoveryPlugin}
 * does not register this type yet. {@link TestPlugin} is a placeholder so single-node tests can load the same plugin list;
 * full discovery wiring for tests is still TBD.
 */

package org.opensearch.test.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elassandra.discovery.CassandraDiscovery;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.MasterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.function.Consumer;

/**
 * Subclass of {@link CassandraDiscovery} that allows tests to wrap publish/resubmit.
 */
public class MockCassandraDiscovery extends CassandraDiscovery {

    public static final String MOCK_CASSANDRA = "mock-cassandra";

    protected final Logger logger = LogManager.getLogger(MockCassandraDiscovery.class);

    Consumer<ClusterChangedEvent> publishFunc;
    Consumer<ClusterChangedEvent> resumitFunc;

    private MockCassandraDiscovery(
        Settings settings,
        TransportService transportService,
        MasterService masterService,
        ClusterService clusterService,
        ClusterApplier clusterApplier,
        ClusterSettings clusterSettings,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(settings, transportService, masterService, clusterService, clusterApplier, clusterSettings, namedWriteableRegistry);
    }

    public void setPublishFunc(Consumer<ClusterChangedEvent> publishFunc) {
        this.publishFunc = publishFunc;
    }

    public void setResumitFunc(Consumer<ClusterChangedEvent> resumitFunc) {
        this.resumitFunc = resumitFunc;
    }

    @Override
    public void publish(
        final ClusterChangedEvent clusterChangedEvent,
        final ActionListener<Void> publishListener,
        final ClusterStatePublisher.AckListener ackListener
    ) {
        if (this.publishFunc != null) {
            this.publishFunc.accept(clusterChangedEvent);
        }
        super.publish(clusterChangedEvent, publishListener, ackListener);
    }

    @Override
    protected void resubmitTaskOnNextChange(final ClusterChangedEvent clusterChangedEvent) {
        if (resumitFunc != null) {
            this.resumitFunc.accept(clusterChangedEvent);
        }
        super.resubmitTaskOnNextChange(clusterChangedEvent);
    }

    /** Marker plugin loaded by {@link org.opensearch.test.ESSingleNodeTestCase} (Elassandra). */
    public static class TestPlugin extends Plugin {
        public TestPlugin(Settings settings) {
            // settings reserved for future wiring
        }
    }
}
