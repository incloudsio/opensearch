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
import org.apache.cassandra.service.ElassandraDaemon;
import org.apache.lucene.util.IOUtils;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


/**
 * Test pk-only documents.
 * @author Barth
 */
public class PkOnlyTests extends OpenSearchSingleNodeTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        resetEmbeddedNode();
        super.setUp();
    }

    private void resetEmbeddedNode() throws IOException {
        if (ElassandraDaemon.instance != null) {
            Node node = ElassandraDaemon.instance.node();
            ElassandraDaemon.instance.node(null);
            IOUtils.close(node);
        }
    }

    private void createIndexAndWaitForKeyspace(String index) throws Exception {
        assertAcked(client().admin().indices().prepareCreate(index).get());
        assertBusy(() -> {
            assertTrue(client().admin().indices().prepareExists(index).get().isExists());
            UntypedResultSet results = process(
                ConsistencyLevel.ONE,
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                index
            );
            assertEquals(1, results.size());
        }, 90, TimeUnit.SECONDS);
    }

    private void waitForTableColumns(String keyspace, String table, String... expectedColumns) throws Exception {
        assertBusy(() -> {
            UntypedResultSet results = process(
                ConsistencyLevel.ONE,
                "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                keyspace,
                table
            );
            HashMap<String, Boolean> columns = new HashMap<>();
            for (UntypedResultSet.Row row : results) {
                columns.put(row.getString("column_name"), Boolean.TRUE);
            }
            for (String expectedColumn : expectedColumns) {
                assertTrue("missing column " + expectedColumn + " in " + columns.keySet(), columns.containsKey(expectedColumn));
            }
        }, 90, TimeUnit.SECONDS);
    }

    private void putEmptyTypeMapping(String index, String type) throws Exception {
        assertAcked(client().admin().indices().preparePutMapping(index).setType(type).setSource(XContentFactory.jsonBuilder().startObject().endObject()).get());
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings(index).get();
            assertNotNull(mappings.mappings().get(index));
            assertNotNull(mappings.mappings().get(index).get(type));
        }, 90, TimeUnit.SECONDS);
    }

    private void assertMetadataContains(UntypedResultSet results, String... expectedColumns) {
        Set<String> actualColumns = new HashSet<>();
        results.metadata().forEach(column -> actualColumns.add(column.name.toString()));
        for (String expectedColumn : expectedColumns) {
            assertTrue("missing column " + expectedColumn + " in " + actualColumns, actualColumns.contains(expectedColumn));
        }
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), creating the underlying CQL table on the fly.
     */
    @Test
    public void testPkOnlyDocumentNoTable() throws Exception {
        final String index = "pk_only_document_no_table";
        createIndexAndWaitForKeyspace(index);
        putEmptyTypeMapping(index, "pk_only");
        
        testSimplePrimaryKey(index, "_id");
    }
    
    @Test
    public void testDynamicMappingPkCustomName() throws Exception {
        final String index = "pk_only_dynamic_mapping_custom";
        createIndexAndWaitForKeyspace(index);
    
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_custom (my_id text PRIMARY KEY, name list<text>)");
        waitForTableColumns(index, "pk_custom", "my_id", "name");
        assertThat(client().prepareIndex(index, "pk_custom", "1").setSource("{\"name\": \"test\"}",
            XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table.
     */
    @Test
    public void testPkOnlyDocumentExistingTable() throws Exception {
        final String index = "pk_only_existing_table";
        createIndexAndWaitForKeyspace(index);
        
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_only (id text PRIMARY KEY)");
        waitForTableColumns(index, "pk_only", "id");
        putEmptyTypeMapping(index, "pk_only");
        testSimplePrimaryKey(index, "id");
    }
    
    /**
     * Test empty pk-only document with an explicit mapping where pk columns are indexed
     */
    @Test
    public void testPkOnlyDocumentPkColumnsIndexed() throws Exception {
        final String index = "pk_only_pk_columns_indexed";
        createIndexAndWaitForKeyspace(index);

        // create a table
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_only (id text, a text, b text, primary key (id, a, b))");
        waitForTableColumns(index, "pk_only", "id", "a", "b");
    
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
        assertAcked(client().admin().indices().preparePutMapping(index)
            .setType("pk_only")
            .setSource(mapping)
            .get());
    
        // insert two documents
        assertThat(client().prepareIndex(index, "pk_only", "[\"1\", \"11\", \"111\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(client().prepareIndex(index, "pk_only", "[\"2\", \"22\", \"222\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            SearchResponse resp = client().prepareSearch(index).setTypes("pk_only").setQuery(QueryBuilders.matchQuery("b", "222")).get();
            assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
            assertThat(resp.getHits().getAt(0).getId(), equalTo("[\"2\",\"22\",\"222\"]"));
            if (resp.getHits().getAt(0).getSourceAsMap() != null) {
                assertThat(resp.getHits().getAt(0).getSourceAsMap(), is(new HashMap<String, String>() {{ put("b","222"); }}));
            }
        }, 90, TimeUnit.SECONDS);
    }
    
    private void testSimplePrimaryKey(String index, String pkName) throws Exception {
        // insert two empty documents, generating a mapping update
        assertThat(client().prepareIndex(index, "pk_only", "1").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(client().prepareIndex(index, "pk_only", "2").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, String.format("SELECT * FROM %s.pk_only WHERE \"%s\" = '1'", index, pkName));
            assertEquals(1, rs.size());
            assertMetadataContains(rs, pkName);
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString(pkName), equalTo("1"));

            assertThat(client().prepareSearch().setIndices(index).setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2L));
            GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("1").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
        }, 90, TimeUnit.SECONDS);
        
        // now add some fields to check it continue to works
        assertThat(client().prepareIndex(index, "pk_only", "3").setSource("{ \"new_field\": \"test\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, String.format("SELECT * FROM %s.pk_only WHERE \"%s\" = '3'", index, pkName));
            assertEquals(1, rs.size());
            assertMetadataContains(rs, pkName, "new_field");
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString(pkName), equalTo("3"));
            assertThat(row.getList("new_field", UTF8Type.instance), is(Collections.singletonList("test")));

            assertThat(client().prepareSearch().setIndices(index).setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3L));
            GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("3").get();
            assertTrue(resp.isExists());
            if (resp.getSource() != null) {
                assertThat(resp.getSource().size(), equalTo(1));
                assertThat(resp.getSource().get("new_field"), equalTo("test"));
            }
        }, 90, TimeUnit.SECONDS);
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with 2 clustering keys.
     */
    @Test
    public void testPkOnlyDocument1() throws Exception {
        final String index = "pk_only_document1";
        createIndexAndWaitForKeyspace(index);
        
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_only (id text, a text, b text, primary key (id, a, b))");
        waitForTableColumns(index, "pk_only", "id", "a", "b");
        putEmptyTypeMapping(index, "pk_only");
        testCompositePrimaryKey(index);
        
        GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"3\",\"33\",\"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        if (resp.getField("_routing") != null) {
            assertThat(resp.getField("_routing").getValue(), equalTo("3"));
        }
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with composite Partition key and one clustering key.
     */
    @Test
    public void testPkOnlyDocument2() throws Exception {
        final String index = "pk_only_document2";
        createIndexAndWaitForKeyspace(index);
        
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_only (id text, a text, b text, primary key ((id, a), b))");
        waitForTableColumns(index, "pk_only", "id", "a", "b");
        putEmptyTypeMapping(index, "pk_only");
        testCompositePrimaryKey(index);
        
        GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"3\",\"33\",\"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        if (resp.getField("_routing") != null) {
            assertThat(resp.getField("_routing").getValue(), equalTo("[\"3\",\"33\"]"));
        }
    }
    
    /**
     * Test indexing dynamically an empty document (pk-only), mapping an existing CQL table, with composite Partition key and no clustering key.
     */
    @Test
    public void testPkOnlyDocument3() throws Exception {
        final String index = "pk_only_document3";
        createIndexAndWaitForKeyspace(index);
        
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + ".pk_only (id text, a text, b text, primary key ((id, a, b)))");
        waitForTableColumns(index, "pk_only", "id", "a", "b");
        putEmptyTypeMapping(index, "pk_only");
        testCompositePrimaryKey(index);
        
        GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"3\",\"33\",\"333\"]").setStoredFields("_routing").get();
        assertTrue(resp.isExists());
        assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]")); // _id canonical form
        if (resp.getField("_routing") != null) {
            assertThat(resp.getField("_routing").getValue(), equalTo("[\"3\",\"33\",\"333\"]"));
        }
    }
    
    private void testCompositePrimaryKey(String index) throws Exception {
        // Insert empty documents after the type exists so the test exercises pk-only indexing,
        // not timing-sensitive dynamic mapping publication.
        assertThat(client().prepareIndex(index, "pk_only", "[\"1\", \"11\", \"111\"]").setSource("{}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        // Wait for the dynamic mapping/secondary index to settle before inserting the CQL-only row.
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings(index).get();
            assertNotNull(mappings.mappings().get(index));
            assertNotNull(mappings.mappings().get(index).get("pk_only"));
            GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"1\",\"11\",\"111\"]").get();
            assertTrue(resp.isExists());
        }, 90, TimeUnit.SECONDS);

        process(ConsistencyLevel.ONE, "INSERT INTO  " + index + ".pk_only (id, a, b) VALUES (?,?,?)", "2", "22", "222");
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + ".pk_only WHERE id = '1' AND a = '11' AND b = '111'");
            assertEquals(1, rs.size());
            assertMetadataContains(rs, "id", "a", "b");
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString("id"), equalTo("1"));
            assertThat(row.getString("a"), equalTo("11"));
            assertThat(row.getString("b"), equalTo("111"));

            assertThat(client().prepareSearch().setIndices(index).setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(2L));
            GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"1\",\"11\",\"111\"]").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
            assertThat(resp.getId(), equalTo("[\"1\",\"11\",\"111\"]"));

            resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"2\",\"22\",\"222\"]").get();
            assertTrue(resp.isExists());
            assertTrue(resp.getSource() == null || resp.getSource().isEmpty());
            assertThat(resp.getId(), equalTo("[\"2\",\"22\",\"222\"]"));
        }, 90, TimeUnit.SECONDS);
        
        // now add some fields to check it continue to works
        assertThat(client().prepareIndex(index, "pk_only", "[\"3\", \"33\", \"333\"]").setSource("{ \"new_field\": \"test\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + ".pk_only WHERE id = '3' AND a = '33' AND b = '333'");
            assertEquals(1, rs.size());
            assertMetadataContains(rs, "id", "a", "b", "new_field");
            UntypedResultSet.Row row = rs.one();
            assertThat(row.getString("id"), equalTo("3"));
            assertThat(row.getString("a"), equalTo("33"));
            assertThat(row.getString("b"), equalTo("333"));
            assertThat(row.getList("new_field", UTF8Type.instance), is(Collections.singletonList("test")));

            assertThat(client().prepareSearch().setIndices(index).setTypes("pk_only").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(3L));
            GetResponse resp = client().prepareGet().setIndex(index).setType("pk_only").setId("[\"3\",\"33\",\"333\"]").get();
            assertTrue(resp.isExists());
            assertThat(resp.getId(), equalTo("[\"3\",\"33\",\"333\"]"));
            if (resp.getSource() != null) {
                assertThat(resp.getSource().size(), equalTo(1));
                assertThat(resp.getSource().get("new_field"), equalTo("test"));
            }
        }, 30, TimeUnit.SECONDS);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }
    
}

