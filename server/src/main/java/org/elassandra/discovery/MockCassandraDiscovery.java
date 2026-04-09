/*
 * Test-oriented Cassandra discovery mock; production code lives in {@code org.elasticsearch.test.discovery}
 * for ES 6.8. For OpenSearch side-car builds, this copy is placed under {@code org.elassandra.discovery} so
 * {@link org.opensearch.discovery.DiscoveryModule} can wire {@code discovery.type: mock-cassandra} without
 * pulling test-framework jars into {@code :server:compileJava}.
 */
package org.elassandra.discovery;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.MasterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.transport.TransportService;

import java.util.function.Consumer;

public class MockCassandraDiscovery extends CassandraDiscovery {

    public static final String MOCK_CASSANDRA = "mock-cassandra";

    private Consumer<ClusterChangedEvent> publishFunc;
    private Consumer<ClusterChangedEvent> resumitFunc;

    public MockCassandraDiscovery(
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
        final AckListener ackListener
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

    /**
     * Forces {@code discovery.type} for single-node tests; production wiring is {@link org.opensearch.discovery.DiscoveryModule}.
     */
    public static class TestPlugin extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("discovery.type", MOCK_CASSANDRA).build();
        }
    }
}
