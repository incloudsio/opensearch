/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for various table options.
 * @author vroyer
 */
public class TableOptionsTests extends OpenSearchSingleNodeTestCase {

    private void waitForKeyspace(String keyspace) throws Exception {
        assertBusy(() -> {
            UntypedResultSet results = process(
                    ConsistencyLevel.ONE,
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                    keyspace
            );
            assertThat(results.size(), equalTo(1));
        }, 30, TimeUnit.SECONDS);
    }

    private void ensureKeyspace(String keyspace) throws Exception {
        process(
                ConsistencyLevel.ONE,
                "CREATE KEYSPACE IF NOT EXISTS "
                        + keyspace
                        + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '"
                        + DatabaseDescriptor.getLocalDataCenter()
                        + "':'1' }"
        );
        waitForKeyspace(keyspace);
    }

    @Test
    public void testTWCS() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("b")
                            .field("type", "double")
                            .field("cql_collection", "singleton")
                        .endObject()
                    .endObject()
                .endObject();

        Settings settings = Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.table_options",
                        "default_time_to_live = 30 " +
                        "AND compaction = {'compaction_window_size': '1', " +
                        "                  'compaction_window_unit': 'MINUTES', " +
                        "                  'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}")
                .build();

        assertAcked(client().admin().indices().prepareCreate("ks").setSettings(settings).addMapping("t1", mapping).get());
        ensureGreen("ks");

        System.out.println("schema version="+StorageService.instance.getSchemaVersion());
        Thread.sleep(3000);
        UntypedResultSet.Row row = process(ConsistencyLevel.ONE, "select * from system_schema.tables where keyspace_name='ks';").one();
        assertThat(row.getInt("default_time_to_live"), equalTo(30));
        Map<String, String> compaction = row.getMap("compaction", UTF8Type.instance, UTF8Type.instance);
        assertThat(compaction.get("class"), equalTo("org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy"));
        assertThat(compaction.get("compaction_window_size"), equalTo("1"));
        assertThat(compaction.get("compaction_window_unit"), equalTo("MINUTES"));
    }
    
    @Test
    public void testDropColulmn() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id").field("type", "keyword").field("cql_collection", "singleton").field("cql_primary_key_order", 0).field("cql_partition_key", true).endObject()
                        .startObject("a").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("b").field("type", "keyword").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject();
        ensureKeyspace("test");
        createIndex("test", Settings.builder().put("index.number_of_replicas", 0).build());
        ensureGreen("test");
        process(ConsistencyLevel.ONE, "CREATE TABLE test.my_type (id text PRIMARY KEY, a text, b text)");
        assertAcked(client().admin().indices().preparePutMapping("test").setType("my_type").setSource(mapping).get());

        XContentBuilder mappingWithoutB = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id").field("type", "keyword").field("cql_collection", "singleton").field("cql_primary_key_order", 0).field("cql_partition_key", true).endObject()
                        .startObject("a").field("type", "keyword").field("cql_collection", "singleton").endObject()
                    .endObject()
                .endObject();
        createIndex("test2", Settings.builder().put("index.keyspace","test").put("index.number_of_replicas", 0).build());
        ensureGreen("test2");
        assertAcked(client().admin().indices().preparePutMapping("test2").setType("my_type").setSource(mappingWithoutB).get());
        
        assertAcked(client().admin().indices().prepareDelete("test").get());
        process(ConsistencyLevel.ONE,"ALTER TABLE test.my_type DROP b");
    }

}

