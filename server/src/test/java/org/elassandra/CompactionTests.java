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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Elassandra SSTable compactions tests.
 * @author vroyer
 *
 */
public class CompactionTests extends OpenSearchSingleNodeTestCase {

    private void createIndexAndWaitForKeyspace(String index) throws Exception {
        createIndexAndWaitForKeyspace(index, Settings.EMPTY);
    }

    private void createIndexAndWaitForKeyspace(String index, Settings settings) throws Exception {
        assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).get());
        assertBusy(() -> {
            assertTrue(client().admin().indices().prepareExists(index).get().isExists());
            UntypedResultSet keyspaces = process(
                    ConsistencyLevel.ONE,
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                    index
            );
            assertThat(keyspaces.size(), equalTo(1));
        }, 90, TimeUnit.SECONDS);
    }
    
    // gradle :core:test -Dtests.seed=C2C04213660E4546 -Dtests.class=org.elassandra.CompactionTests -Dtests.method="expiredTtlColumnCompactionTest" -Dtests.security.manager=false -Dtests.locale=zh -Dtests.timezone=Canada/Eastern
    @Test
    public void basicCompactionTest() throws Exception {
        final String firstIndex = "compaction_basic_test1";
        final String secondIndex = "compaction_basic_test2";
        createIndexAndWaitForKeyspace(firstIndex);
        createIndexAndWaitForKeyspace(secondIndex);
        
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + firstIndex + ".t1 ( a int, b text, primary key (a) ) WITH "+
                "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}");
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + secondIndex + ".t2 ( a int, b text, c int, primary key ((a),b) ) WITH "+
                "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}");
        XContentBuilder mappingt1 = XContentFactory.jsonBuilder().startObject().startObject("t1").field("discover",".*").endObject().endObject();
        XContentBuilder mappingt2 = XContentFactory.jsonBuilder().startObject().startObject("t2").field("discover",".*").endObject().endObject();
        
        assertAcked(client().admin().indices().preparePutMapping(firstIndex).setType("t1").setSource(mappingt1).get());
        assertAcked(client().admin().indices().preparePutMapping(secondIndex).setType("t2").setSource(mappingt2).get());
        
        Map<String, Gauge> gaugest1 = CassandraMetricsRegistry.Metrics.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.endsWith("t1");
            }
        });
        Map<String, Gauge> gaugest2 = CassandraMetricsRegistry.Metrics.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.endsWith("t2");
            }
        });
        
        int i=0;
        for(int j=0 ; j < 100; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + firstIndex + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "x", i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "y", i);
        }
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(100L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*100L));
        StorageService.instance.forceKeyspaceFlush(firstIndex,"t1");
        StorageService.instance.forceKeyspaceFlush(secondIndex,"t2");
        
        for(String s:gaugest1.keySet())
            System.out.println(s+"="+gaugest1.get(s).getValue());
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(1));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(1));
        
        for(int j=0 ; j < 100; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + firstIndex + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "x", i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "y", i);
        }
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(200L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*200L));
        StorageService.instance.forceKeyspaceFlush(firstIndex,"t1");
        StorageService.instance.forceKeyspaceFlush(secondIndex,"t2");
        Thread.sleep(200);
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(2));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(2));
        
        for(int j=0 ; j < 100; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + firstIndex + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "x", i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", i, "y", i);
        }
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(300L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*300L));
        StorageService.instance.forceKeyspaceFlush(firstIndex);
        StorageService.instance.forceKeyspaceFlush(secondIndex);
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(3));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(3));
        
        // force compaction
        StorageService.instance.forceKeyspaceCompaction(true, firstIndex);
        StorageService.instance.forceKeyspaceCompaction(true, secondIndex);
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(300L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*300L));
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(1));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(1));
        
        for(String s:gaugest1.keySet())
            System.out.println(s+"="+gaugest1.get(s).getValue());
        
        // overwrite 100 docs
        for(int j=0 ; j < 100; j++) {
            process(ConsistencyLevel.ONE,"insert into " + firstIndex + ".t1 (a,b) VALUES (?,?)", 100+j, "y");
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)", 100+j, "x", i);
            process(ConsistencyLevel.ONE,"insert into " + secondIndex + ".t2 (a,b,c) VALUES (?,?,?)",100+j, "y", i);
        }
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(300L));
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(100L));
        
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*300L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(300L));
        
        StorageService.instance.forceKeyspaceFlush(firstIndex);
        StorageService.instance.forceKeyspaceFlush(secondIndex);
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(2));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(2));
        StorageService.instance.forceKeyspaceCompaction(true, firstIndex);
        StorageService.instance.forceKeyspaceCompaction(true, secondIndex);
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(1));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(1));
        
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(300L));
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(100L));
        
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*300L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(300L));
        
        // remove 100 docs
        for(int j=0 ; j < 100; j++) {
            process(ConsistencyLevel.ONE,"delete from " + firstIndex + ".t1 WHERE a = ?", 100+j);
            process(ConsistencyLevel.ONE,"delete from " + secondIndex + ".t2 WHERE a = ? and b = ?", 100+j, "x");
            process(ConsistencyLevel.ONE,"delete from " + secondIndex + ".t2 WHERE a = ? and b = ?", 100+j, "y");
        }
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(200L));
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(0L));

        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*200L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(200L));

        StorageService.instance.forceKeyspaceFlush(firstIndex);
        StorageService.instance.forceKeyspaceFlush(secondIndex);
        StorageService.instance.forceKeyspaceCompaction(true, firstIndex);
        StorageService.instance.forceKeyspaceCompaction(true, secondIndex);
        assertThat(gaugest1.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + firstIndex + ".t1").getValue(), equalTo(1));
        assertThat(gaugest2.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + secondIndex + ".t2").getValue(), equalTo(1));
        
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(200L));
        assertThat(client().prepareSearch().setIndices(firstIndex).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(0L));
        
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*200L));
        assertThat(client().prepareSearch().setIndices(secondIndex).setTypes("t2").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(200L));
    }
    
    @Test
    public void expiredTtlCompactionTest() throws Exception {
        final String index = "expired_ttl_compaction";
        createIndexAndWaitForKeyspace(index, Settings.builder().put(IndexMetadata.SETTING_INDEX_ON_COMPACTION, true).build());
        
        long N = 10;
        
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + index + ".t1 ( a int,b text, primary key (a) ) WITH "+
                "gc_grace_seconds = 15 " +
                " AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}");
        assertAcked(client().admin().indices().preparePutMapping(index).setType("t1")
                .setSource("{ \"t1\" : { \"discover\" : \".*\", \"_meta\": { \"index_on_compaction\":true } }}", XContentType.JSON).get());
        
        Map<String, Gauge> gauges = CassandraMetricsRegistry.Metrics.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.endsWith("t1");
            }
        });
        
        int i=0;
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(N));
        StorageService.instance.forceKeyspaceFlush(index,"t1");
        
        for(String s:gauges.keySet())
            System.out.println(s+"="+gauges.get(s).getValue());
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
        
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?) USING TTL 15", i, "y");
        }
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(N));
        StorageService.instance.forceKeyspaceFlush(index,"t1");
        Thread.sleep(2000);
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(2));
        
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?)", i, "x"+i);
        }
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(N));
        StorageService.instance.forceKeyspaceFlush(index);
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(3));
        
        // force compaction
        StorageService.instance.forceKeyspaceCompaction(true, index);
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(N));
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
        
        for(String s:gauges.keySet())
            System.out.println(s+"="+gauges.get(s).getValue());
       
        
        Thread.sleep(15*1000);  // wait TTL expiration
        Thread.sleep(20*1000);  // wait gc_grace_seconds expiration
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(N));

        StorageService.instance.forceKeyspaceFlush(index);
        StorageService.instance.forceKeyspaceCompaction(true, index);
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
        
        UntypedResultSet rs = process(ConsistencyLevel.ONE,"SELECT * FROM " + index + ".t1");
        System.out.println("t1.count = "+rs.size());
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.queryStringQuery("b:y")).get().getHits().getTotalHits().value, equalTo(0L));
    }
    
    // gradle :core:test -Dtests.seed=C2C04213660E4546 -Dtests.class=org.elassandra.CompositeTests -Dtests.method="testReadBeforeWrite" -Dtests.security.manager=false -Dtests.locale=zh-TW -Dtests.timezone=Pacific/Pitcairn
    @Test
    public void expiredTtlColumnCompactionTest() throws Exception {
        final String index = "expired_ttl_column_compaction";
        createIndexAndWaitForKeyspace(index, Settings.builder()
                .put("index.refresh_interval", -1)
                .put(IndexMetadata.SETTING_INDEX_ON_COMPACTION, true)
                .build());
        
        long N = 10;
        
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS " + index + ".t1 ( a int,b text, c text, primary key (a) ) WITH "+
                "gc_grace_seconds = 15 " +
                " AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}");
        assertAcked(client().admin().indices().preparePutMapping(index).setType("t1")
                .setSource("{ \"t1\" : { \"discover\" : \".*\" }}", XContentType.JSON).get());
        
        Map<String, Gauge> gauges = CassandraMetricsRegistry.Metrics.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.endsWith("t1");
            }
        });
        
        int i=0;
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b,c) VALUES (?,?,?)", i, "b"+i, "c"+i);
        }
        StorageService.instance.forceKeyspaceFlush(index,"t1");
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(N));
        
        for(String s:gauges.keySet())
            System.out.println(s+"="+gauges.get(s).getValue());
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
        
        for(int j=0 ; j < N; j++) {
            i++;
            process(ConsistencyLevel.ONE,"insert into " + index + ".t1 (a,b) VALUES (?,?) USING TTL 15", i, "b"+i);
        }
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("b", "*")).get().getHits().getTotalHits().value, equalTo(2*N));
        StorageService.instance.forceKeyspaceFlush(index,"t1");
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(2));
        
        // force compaction
        StorageService.instance.forceKeyspaceCompaction(true, index);
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("b","*")).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
       
        Thread.sleep(15*1000);  // wait TTL expiration
        Thread.sleep(20*1000);  // wait gc_grace_seconds expiration
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2*N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("c","*")).get().getHits().getTotalHits().value, equalTo(N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("b","*")).get().getHits().getTotalHits().value, equalTo(2*N));

        StorageService.instance.forceKeyspaceFlush(index);
        StorageService.instance.forceKeyspaceCompaction(true, index);
        assertThat(gauges.get("org.apache.cassandra.metrics.Table.LiveSSTableCount." + index + ".t1").getValue(), equalTo(1));
        
        UntypedResultSet rs = process(ConsistencyLevel.ONE,"SELECT * FROM " + index + ".t1");
        System.out.println("t1.count = "+rs.size());
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("c","*")).get().getHits().getTotalHits().value, equalTo(N));
        assertThat(client().prepareSearch().setIndices(index).setTypes("t1").setQuery(QueryBuilders.wildcardQuery("b","*")).get().getHits().getTotalHits().value, equalTo(N));
    }

}
