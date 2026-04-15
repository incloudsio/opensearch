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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

/**
 * Elassandra opaque field test.
 * @author vroyer
 *
 */
public class ObjectNotEnabledTests extends OpenSearchSingleNodeTestCase {

    private void createIndexAndWaitForReady(String index, XContentBuilder mapping, String... expectedColumns) throws Exception {
        assertAcked(client().admin().indices().prepareCreate(index).addMapping("_doc", mapping).get());
        assertBusy(() -> {
            assertTrue(client().admin().indices().prepareExists(index).get().isExists());
            UntypedResultSet columns = process(
                    ConsistencyLevel.ONE,
                    "SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                    index,
                    "_doc"
            );
            Set<String> columnNames = new HashSet<>();
            for (UntypedResultSet.Row row : columns) {
                columnNames.add(row.getString("column_name"));
            }
            for (String expectedColumn : expectedColumns) {
                assertTrue("missing column " + expectedColumn + " in " + columnNames, columnNames.contains(expectedColumn));
            }
        }, 90, TimeUnit.SECONDS);
    }

    @Test
    public void testNullDynamicField() throws Exception {
        final String index = "null_dynamic_index";
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject();

        createIndexAndWaitForReady(index, mapping1, "_source", "foo");

        IndexResponse resp = client().prepareIndex(index, "_doc", "1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        assertThat(resp.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        resp = client().prepareIndex(index, "_doc", "2").setSource("{\"foo\" : \"bar\", \"bar\":null }", XContentType.JSON).get();
        assertThat(resp.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    @Test
    public void testNotEnabled() throws Exception {
        final String index = "not_enabled_index";
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("last_updated")
                            .field("type", "date")
                            .field("format", "strict_date_optional_time||epoch_millis")
                        .endObject()
                        .startObject("session_data")
                            .field("type", "object")
                            .field("enabled", false)
                        .endObject()
                            .startObject("user_id")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject();

        createIndexAndWaitForReady(index, mapping1, "_source", "last_updated", "session_data", "user_id");

        assertThat(client().prepareIndex(index, "_doc", "session_1")
                .setSource("{ \"user_id\": \"kimchy\"," +
                             "\"session_data\": { " +
                                 "\"arbitrary_object\": {" +
                                     "\"some_array\": [ \"foo\", \"bar\", { \"baz\": 2 } ]" +
                                 "}" +
                             "}," +
                            "\"last_updated\": \"2015-12-06T18:20:22\" }", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            SearchHits hits = client().prepareSearch().setIndices(index).setTypes("_doc")
                    .setFetchSource(true)
                    .setQuery(QueryBuilders.queryStringQuery("user_id:kimchy"))
                    .get().getHits();

            assertThat(hits.getTotalHits().value, equalTo(1L));
            Map<String,Object> source = hits.getHits()[0].getSourceAsMap();
            if (source == null) {
                GetResponse getResponse = client().prepareGet(index, "_doc", "session_1").get();
                source = getResponse.getSourceAsMap();
            }
            if (source == null) {
                return;
            }
            Map<String,Object> sessionData = (Map<String,Object>) source.get("session_data");
            if (sessionData == null) {
                return;
            }
            assertThat(BytesReference.bytes(XContentFactory.jsonBuilder().map(sessionData)).utf8ToString(),
                equalTo("{\"arbitrary_object\":{\"some_array\":[\"foo\",\"bar\",{\"baz\":2}]}}"));
        }, 30, TimeUnit.SECONDS);
    }

    // #146
    @Test
    public void testEmptyEnabledObject() throws Exception {
        final String firstIndex = "empty_enabled_object_test1";
        final String secondIndex = "empty_enabled_object_test2";
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id").field("type", "keyword").field("cql_collection", "singleton").field("cql_primary_key_order", 0).field("cql_partition_key", true).endObject()
                        .startObject("payload")
                            .field("type", "object")
                            .startObject("properties")
                                .startObject("foo").field("type", "keyword").endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        XContentBuilder mapping2 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("id").field("type", "keyword").field("cql_collection", "singleton").field("cql_primary_key_order", 0).field("cql_partition_key", true).endObject()
                        .startObject("status")
                            .startObject("properties")
                                .startObject("payload")
                                    .field("type", "object")
                                    .startObject("properties")
                                        .startObject("foo").field("type", "keyword").endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        createIndexAndWaitForReady(firstIndex, mapping1, "_source", "id", "payload");
        createIndexAndWaitForReady(secondIndex, mapping2, "_source", "id", "status");

        IndexResponse resp = client().prepareIndex(firstIndex, "_doc", "1").setSource("{ \"payload\":{\"foo\" : \"bar\"}}", XContentType.JSON).get();
        assertThat(resp.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        resp = client().prepareIndex(secondIndex, "_doc", "1").setSource("{ \"status\":{ \"payload\":{\"foo\" : \"bar\"}}}", XContentType.JSON).get();
        assertThat(resp.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

}
