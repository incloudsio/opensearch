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
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Elassandra index rebuild tests.
 * Index rebuild rely on the compaction manger to read SSTables (Does NOT rebuild in-memory data).
 * @author vroyer
 *
 */
//gradle :server:test -Dtests.seed=65E2CF27F286CC89 -Dtests.class=org.elassandra.IndexBuildTests -Dtests.security.manager=false -Dtests.locale=en-PH -Dtests.timezone=America/Coral_Harbour
public class IndexBuildTests extends OpenSearchSingleNodeTestCase {
    static long N = 10;

    private String randomIndexName(String prefix) {
        return (prefix + "_" + randomAlphaOfLength(8)).toLowerCase(Locale.ROOT);
    }

    private void closeIndex(String index) throws Exception {
        client().admin().indices().prepareClose(index).get();
    }

    private void openIndex(String index) throws Exception {
        client().admin().indices().prepareOpen(index).get();
    }

    private void assertSearchHitCount(String index, long expected) throws Exception {
        assertBusy(() -> assertThat(
            client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value,
            equalTo(expected)
        ));
    }

    @Test
    public void indexRebuildTest() throws Exception {
        indexRebuild(1);
    }

    @Test
    public void indexMultithreadRebuildTest() throws Exception {
        indexRebuild(3);
    }

    public void indexRebuild(int numThread) throws Exception {
        String index = randomIndexName("test");
        createIndex(index);
        ensureGreen(index);

        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + index + ".t1 ( a int,b text, primary key (a) )");

        assertAcked(client().admin().indices().preparePutMapping(index).setType("t1").setSource(discoverMapping("t1")).get());
        int i=0;
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }
        assertSearchHitCount(index, N);

        // close index
        closeIndex(index);

        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }
        UntypedResultSet rs = process(ConsistencyLevel.ONE,"select count(*) from " + index + ".t1");
        StorageService.instance.forceKeyspaceFlush(index,"t1");

        // open index
        openIndex(index);
        ensureGreen(index);

        assertSearchHitCount(index, N);

        // rebuild_index
        StorageService.instance.rebuildSecondaryIndex(numThread, index, "t1", "elastic_t1_idx");
        assertTrue(waitIndexRebuilt(index, Collections.singletonList("t1"), 15000));
        assertSearchHitCount(index, 2 * N);
    }

    @Test
    public void indexFirstBuildTest() throws Exception {
        String index = randomIndexName("test");
        createIndex(index);
        ensureGreen(index);

        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + index + ".t1 ( a int,b text, primary key (a) )");
        int i=0;
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }
        StorageService.instance.forceKeyspaceFlush(index,"t1");

        assertAcked(client().admin().indices().preparePutMapping(index).setType("t1").setSource(discoverMapping("t1")).get());
        assertTrue(waitIndexRebuilt(index, Collections.singletonList("t1"), 15000));

        assertSearchHitCount(index, N);
    }

    @Test
    public void indexWithReplicationMap() throws Exception {
        String indexName = "test_rep";
        createIndex(indexName, Settings.builder().putList(IndexMetadata.SETTING_REPLICATION, "DC1:1","DC2:2").build());
        ensureGreen(indexName);
        UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?", indexName);
        Map<String, String> replication = rs.one().getMap("replication", UTF8Type.instance, UTF8Type.instance);
        System.out.println("replication="+replication);
        assertThat(replication.get("class"), equalTo("org.apache.cassandra.locator.NetworkTopologyStrategy"));
        assertThat(replication.get("DC1"), equalTo("1"));
        assertThat(replication.get("DC2"), equalTo("2"));
    }

    @Test
    public void testDelayedIndexBuild() throws Exception {
        String index = randomIndexName("test");
        process(
            ConsistencyLevel.ONE,
            String.format(
                Locale.ROOT,
                "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '%s':'1' }",
                index,
                DatabaseDescriptor.getLocalDataCenter()
            )
        );
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + index + ".t1 ( a int,b text, primary key (a) )");
        int i=0;
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }

        process(ConsistencyLevel.ONE,"CREATE CUSTOM INDEX elastic_t1_idx ON " + index + ".t1 () USING 'org.elassandra.index.ExtendedElasticSecondaryIndex';");
        assertFalse(waitIndexRebuilt(index, Collections.singletonList("t1"), 5000));

        createIndex(index);
        ensureGreen(index);
        assertAcked(client().admin().indices().preparePutMapping(index).setType("t1").setSource(discoverMapping("t1")).get());

        assertTrue(waitIndexRebuilt(index, Collections.singletonList("t1"), 15000));
        assertSearchHitCount(index, N);
    }
}
