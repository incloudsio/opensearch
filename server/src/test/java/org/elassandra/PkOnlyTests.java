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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


/**
 * Test pk-only documents.
 * @author Barth
 */
public class PkOnlyTests extends OpenSearchSingleNodeTestCase {

    private void createIndexAndWaitForKeyspace(String index) throws Exception {
        createIndex(index);
        ensureGreen(index);
        assertBusy(() -> {
            UntypedResultSet results = process(
                ConsistencyLevel.ONE,
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                index
            );
            assertEquals(1, results.size());
        }, 90, TimeUnit.SECONDS);
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), creating the underlying CQL table on the fly.
     */
    @Test
    public void testPkOnlyDocumentNoTable() throws Exception {
        createIndexAndWaitForKeyspace("test1");
        
        testSimplePrimaryKey("_id");
    }
    
    @Test
    public void testDynamicMappingPkCustomName() throws Exception {
        createIndexAndWaitForKeyspace("test1");
    
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_custom (my_id text PRIMARY KEY, name list<text>)");
        assertThat(client().prepareIndex("test1", "pk_custom", "1").setSource("{\"name\": \"test\"}",
            XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table.
     */
    @Test
    public void testPkOnlyDocumentExistingTable() throws Exception {
        createIndexAndWaitForKeyspace("test1");
        
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_only (id text PRIMARY KEY)");
        testSimplePrimaryKey("id");
    }
    
    /**
     * Test empty pk-only document with an explicit mapping where pk columns are indexed
     */
    @Test
    public void testPkOnlyDocumentPkColumnsIndexed() throws Exception {
        createIndexAndWaitForKeyspace("test1");

        // create a table
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_only (id text, a text, b text, primary key (id, a, b))");
    
        // put a mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("b")
                        .field("type", "keyword")
                        .field("cql_collection", "singleton")
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(client().admin().indices().preparePutMapping("test1")
            .setType("pk_only")
            .setSource(mapping)
            .get());
    
        // insert two documents
        assertThat(client().prepareIndex("test1", "pk_only", "[\"1\", \"11\", \"111\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(client().prepareIndex("test1", "pk_only", "[\"2\", \"22\", \"222\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("test1").get();
            SearchResponse resp = client().prepareSearch("test1").setTypes("pk_only").setQuery(QueryBuilders.matchQuery("b", "222")).get();
            assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("[\"2\",\"22\",\"222\"]"));
            assertThat(resp.getHits().getAt(0).getSourceAsMap(), is(new HashMap<String, String>() {{ put("b","222"); }}));
        }, 90, TimeUnit.SECONDS);
    }
    
    private void testSimplePrimaryKey(String pkName) throws Exception {
        // insert two empty documents, generating a mapping update
        assertThat(client().prepareIndex("test1", "pk_only", "1").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(client().prepareIndex("test1", "pk_only", "2").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("test1").get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, String.format("SELECT * FROM test1.pk_only WHERE \"%s\" = '1'", pkName));
            assertEquals(1, rs.size());
            assertEquals(1, rs.metadata().size());
            assertThat(rs.metadata().get(0).name.toString(), equalTo(pkName));
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString(pkName), equalTo("1"));

            assertThat(client().prepareSearch().setIndices("test1").setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2L));
            GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("1").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
        }, 90, TimeUnit.SECONDS);
        
        // now add some fields to check it continue to works
        assertThat(client().prepareIndex("test1", "pk_only", "3").setSource("{ \"new_field\": \"test\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        assertBusy(() -> {
            client().admin().indices().prepareRefresh("test1").get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, String.format("SELECT * FROM test1.pk_only WHERE \"%s\" = '3'", pkName));
            assertEquals(1, rs.size());
            assertEquals(2, rs.metadata().size());
            assertThat(rs.metadata().get(0).name.toString(), equalTo(pkName));
            assertThat(rs.metadata().get(1).name.toString(), equalTo("new_field"));
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString(pkName), equalTo("3"));
            assertThat(row.getList("new_field", UTF8Type.instance), is(Collections.singletonList("test")));

            assertThat(client().prepareSearch().setIndices("test1").setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3L));
            GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("3").get();
            assertTrue(resp.isExists());
            assertThat(resp.getSource().size(), equalTo(1));
            assertThat(resp.getSource().get("new_field"), equalTo("test"));
        }, 90, TimeUnit.SECONDS);
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with 2 clustering keys.
     */
    @Test
    public void testPkOnlyDocument1() throws Exception {
        createIndexAndWaitForKeyspace("test1");
        
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_only (id text, a text, b text, primary key (id, a, b))");
        testCompositePrimaryKey();
        
        GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"3\", \"33\", \"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        assertThat(resp.getField("_routing").getValue(), equalTo("3"));
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with composite Partition key and one clustering key.
     */
    @Test
    public void testPkOnlyDocument2() throws Exception {
        createIndexAndWaitForKeyspace("test1");
        
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_only (id text, a text, b text, primary key ((id, a), b))");
        testCompositePrimaryKey();
        
        GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"3\", \"33\", \"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        assertThat(resp.getField("_routing").getValue(), equalTo("[\"3\",\"33\"]"));
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with composite Partition key and no clustering key.
     */
    @Test
    public void testPkOnlyDocument3() throws Exception {
        createIndexAndWaitForKeyspace("test1");
        
        process(ConsistencyLevel.ONE,"CREATE TABLE test1.pk_only (id text, a text, b text, primary key ((id, a, b)))");
        testCompositePrimaryKey();
        
        GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"3\", \"33\", \"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        assertThat(resp.getField("_routing").getValue(), equalTo("[\"3\",\"33\",\"333\"]"));
    }
    
    private void testCompositePrimaryKey() throws Exception {
        // insert two empty documents, generating a mapping update
        assertThat(client().prepareIndex("test1", "pk_only", "[\"1\", \"11\", \"111\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        process(ConsistencyLevel.ONE, "INSERT INTO  test1.pk_only (id, a, b) VALUES (?,?,?)", "2", "22", "222");
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("test1").get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM test1.pk_only WHERE id = '1' AND a = '11' AND b = '111'");
            assertEquals(1, rs.size());
            assertEquals(3, rs.metadata().size());
            assertThat(rs.metadata().get(0).name.toString(), equalTo("id"));
            assertThat(rs.metadata().get(1).name.toString(), equalTo("a"));
            assertThat(rs.metadata().get(2).name.toString(), equalTo("b"));
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString("id"), equalTo("1"));
            assertThat(row.getString("a"), equalTo("11"));
            assertThat(row.getString("b"), equalTo("111"));

            assertThat(client().prepareSearch().setIndices("test1").setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2L));
            GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"1\", \"11\", \"111\"]").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
            assertThat(resp.getId(), equalTo("[\"1\",\"11\",\"111\"]"));

            resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"2\",\"22\",\"222\"]").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
            assertThat(resp.getId(), equalTo("[\"2\",\"22\",\"222\"]"));
        }, 90, TimeUnit.SECONDS);
        
        // now add some fields to check it continue to works
        assertThat(client().prepareIndex("test1", "pk_only", "[\"3\", \"33\", \"333\"]").setSource("{ \"new_field\": \"test\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("test1").get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM test1.pk_only WHERE id = '3' AND a = '33' AND b = '333'");
            assertEquals(1, rs.size());
            assertEquals(4, rs.metadata().size());
            assertThat(rs.metadata().get(0).name.toString(), equalTo("id"));
            assertThat(rs.metadata().get(1).name.toString(), equalTo("a"));
            assertThat(rs.metadata().get(2).name.toString(), equalTo("b"));
            assertThat(rs.metadata().get(3).name.toString(), equalTo("new_field"));
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString("id"), equalTo("3"));
            assertThat(row.getString("a"), equalTo("33"));
            assertThat(row.getString("b"), equalTo("333"));
            assertThat(row.getList("new_field", UTF8Type.instance), is(Collections.singletonList("test")));

            assertThat(client().prepareSearch().setIndices("test1").setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3L));
            GetResponse resp = client().prepareGet().setIndex("test1").setType("pk_only").setId("[\"3\", \"33\", \"333\"]").get();
            assertTrue(resp.isExists());
            assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]"));
            assertThat(resp.getSource().size(), equalTo(1));
            assertThat(resp.getSource().get("new_field"), equalTo("test"));
        }, 30, TimeUnit.SECONDS);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }
    
}

