/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.elassandra.discovery.MockCassandraDiscovery;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CassandraDiscoveryTests extends OpenSearchSingleNodeTestCase {

    long version = -1;
    int resumit = 0;

    final List<String> MAPPING_COLLECTIONS = Arrays.asList("list","set","singleton");
    final List<String> MAPPING_TYPES = Arrays.asList("keyword",
        "text",
        "date",
        "byte",
        "short",
        "integer",
        "long",
        "double",
        "float",
        "boolean",
        "binary",
        "ip",
        "geo_point",
        "geo_shape");

    @Override
    public void tearDown() throws Exception {
        MockCassandraDiscovery discovery = this.getMockCassandraDiscovery();
        discovery.setPublishFunc(null);
        discovery.setResumitFunc(null);
        super.tearDown();
    }

    @Test
    public void testConcurrentPaxosUpdate() {
        createIndex("test");
        ensureGreen("test");

        MockCassandraDiscovery discovery = this.getMockCassandraDiscovery();
        final UUID concurrentOwner = UUID.randomUUID();
        discovery.setPublishFunc(e -> {
            if (version == -1) {
                // Inject a conflicting owner into the exact Paxos slot that the next schema update will reserve.
                // The publish hook fires on a shard-started update first, so the in-memory metadata version is ahead
                // of the persisted metadata_log by one already; the later schema publish reserves version + 2.
                version = e.state().metadata().version();
                final long forcedVersion = version + 2;
                logger.warn("forcing metadata.version to {} in elastic_admin.metadata",  forcedVersion);

                process(ConsistencyLevel.ONE, "UPDATE elastic_admin.metadata_log SET owner = ?, version = ?, source = ?, ts = dateOf(now()) WHERE cluster_name = ? AND v = ?",
                        concurrentOwner,
                        forcedVersion,
                        "paxos test",
                        DatabaseDescriptor.getClusterName(),
                        forcedVersion);
            }
        });

        discovery.setResumitFunc(e -> {
            if (resumit == 0) {
                resumit++;
                final Metadata replayMetaData = e.state().metadata();
                // publish
                clusterService().submitStateUpdateTask("cql-schema-mapping-update", new ClusterStateUpdateTask() {

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        ClusterState.Builder newStateBuilder = ClusterState.builder(currentState);
                        Metadata newMetadata = Metadata.builder(replayMetaData)
                                .persistentSettings(Settings.builder().build())
                                .clusterUUID(currentState.metadata().clusterUUID())
                                .version(currentState.metadata().version() + 1)
                                .build();
                        ClusterState newClusterState = newStateBuilder.incrementVersion().metadata(newMetadata).build();
                        logger.warn("submit cql-schema-mapping-update metadata.version={}",  newClusterState.metadata().version());
                        return newClusterState;
                    }

                    @Override
                    public void onFailure(String source, Exception t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }

                });
                discovery.setResumitFunc(null);
            }
        });

        assertThat(client().prepareIndex("test", "my_type", "1").setSource("{\"status_code\": \"OK\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertEquals(1, resumit);
        final long currentVersion = this.clusterService().state().metadata().version();
        assertThat(currentVersion, greaterThanOrEqualTo(version + 2));
        process(
            ConsistencyLevel.ONE,
            "UPDATE elastic_admin.metadata_log SET owner = ?, version = ?, source = ?, ts = dateOf(now()) WHERE cluster_name = ? AND v = ?",
            UUID.fromString(StorageService.instance.getLocalHostId()),
            currentVersion,
            "paxos test cleanup",
            DatabaseDescriptor.getClusterName(),
            currentVersion
        );
    }



    @Test
    public void indexThousandsOfFields() throws Exception {
        final int NB_FIELDS = 5000;

        XContentBuilder mappingDef = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                        .field("cql_primary_key_order", 0)
                        .field("cql_partition_key", true)
                    .endObject();

        for (int i = 1; i <= NB_FIELDS; ++i) {
            mappingDef = mappingDef.startObject(String.format("c%05d", i))
                .field("type", randomFrom(MAPPING_TYPES))
                .field("cql_collection", randomFrom(MAPPING_COLLECTIONS))
                .field("cql_partition_key", false)
                .endObject();
        }

        mappingDef.endObject()
                .startObject("_meta")
                    .field("index_static_columns", false)
                    .field("index_insert_only", true)
                .endObject()
            .endObject();

        assertAcked(client().admin().indices().prepareCreate("test1")
            .setSettings(Settings.builder()
                .put("index.keyspace","test")
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), (NB_FIELDS+1000)).build()) // force the max fields to higher value
            .addMapping("t1", mappingDef));
        ensureGreen("test1");

        // query C* to retrieve the Mapping from schema extension.
        UntypedResultSet urs = process(ConsistencyLevel.ONE, "SELECT extensions from system_schema.tables where keyspace_name = ? and table_name = ?", "test", "t1");
        assertEquals(1, urs.size());
        Map<String, ByteBuffer> extensionsMap = urs.one().getMap("extensions", UTF8Type.instance, BytesType.instance);
        assertNotNull(extensionsMap);

        IndexMetadata indexMetaDataTest1 = clusterService().getIndexMetaDataFromExtension(extensionsMap.get("elastic_admin/test1"));
        MappingMetadata mmd = indexMetaDataTest1.getMappings().get("t1");
        assertNotNull(mmd);

        Map<String, Object> mapping = mmd.getSourceAsMap();
        assertNotNull(mapping);

        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertNotNull(properties);

        assertTrue(properties.containsKey("id"));
        for (int i = 1; i <= NB_FIELDS; ++i) {
            assertTrue(properties.containsKey(String.format("c%05d", i)));
        }

        final long currentVersion = this.clusterService().state().metadata().version();
        process(
            ConsistencyLevel.ONE,
            "UPDATE elastic_admin.metadata_log SET owner = ?, version = ?, source = ?, ts = dateOf(now()) WHERE cluster_name = ? AND v = ?",
            UUID.fromString(StorageService.instance.getLocalHostId()),
            currentVersion,
            "indexThousandsOfFields cleanup",
            DatabaseDescriptor.getClusterName(),
            currentVersion
        );
        assertAcked(client().admin().indices().prepareDelete("test1"));
    }
}
