/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Elassandra side-car: replaces stock OpenSearch Gateway.performStateRecovery (Zen transport +
 * minimum_master_nodes quorum over TransportNodesListGatewayMetaState). Embedded mock-cassandra
 * tests never satisfy that path, so listener.onFailure fires and STATE_NOT_RECOVERED_BLOCK stays.
 * This matches the Elasticsearch fork: metadata via ClusterService.loadGlobalState() with bootstrap UUID.
 */
package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

public class Gateway {

    private static final Logger logger = LogManager.getLogger(Gateway.class);

    private final ClusterService clusterService;

    @SuppressWarnings("unused")
    public Gateway(
        final Settings settings,
        final ClusterService clusterService,
        final TransportNodesListGatewayMetaState listGatewayMetaState
    ) {
        this.clusterService = clusterService;
    }

    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        final ClusterState.Builder builder = ClusterState.builder(clusterService.state());
        try {
            final Metadata metadata = clusterService.loadGlobalState();
            logger.info(
                "Successful cluster state recovery from metadata store version={}/{}",
                metadata.clusterUUID(),
                metadata.version()
            );
            listener.onSuccess(builder.metadata(metadata).build());
        } catch (final Exception e) {
            Metadata meta = clusterService.state().metadata();
            if ("_na_".equals(meta.clusterUUID())) {
                meta = Metadata.builder(meta).clusterUUID(clusterService.localNode().getId()).build();
            }
            logger.info(
                "Bootstrap cluster metadata after loadGlobalState failure version={}/{} ({})",
                meta.clusterUUID(),
                meta.version(),
                e.toString()
            );
            listener.onSuccess(builder.metadata(meta).build());
        }
    }

    public interface GatewayStateRecoveredListener {
        void onSuccess(ClusterState build);

        void onFailure(String s);
    }
}
