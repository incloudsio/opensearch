package org.elassandra.discovery;

/**
 * Replaces Elasticsearch {@code DiscoveryNode.DiscoveryNodeStatus} (removed in OpenSearch) for gossip-driven discovery.
 */
public enum ElassandraGossipNodeStatus {
    UNKNOWN,
    ALIVE,
    DISABLED,
    DEAD;

    public boolean isAlive() {
        return this == ALIVE;
    }
}
