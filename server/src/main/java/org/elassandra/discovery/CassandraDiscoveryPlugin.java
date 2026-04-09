/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 */

package org.elassandra.discovery;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.Plugin;

/**
 * Elasticsearch 6.8 registered Cassandra discovery via {@code DiscoveryPlugin#getDiscoveryTypes}. OpenSearch 1.3 removed
 * that SPI; Elassandra wires {@link CassandraDiscovery} from {@code DiscoveryModule} when {@code discovery.type} is
 * {@value #CASSANDRA} (see {@code scripts/patch-opensearch-discovery-module-elassandra.sh} for the side-car checkout).
 */
public class CassandraDiscoveryPlugin extends Plugin implements DiscoveryPlugin {

    public static final String CASSANDRA = "cassandra";

    private final Settings settings;

    public CassandraDiscoveryPlugin(Settings settings) {
        this.settings = settings;
    }
}
