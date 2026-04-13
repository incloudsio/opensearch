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
import org.opensearch.action.DocWriteResponse;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Elassandra Naming tests.
 * @author vroyer
 *
 */
public class NamingTests extends OpenSearchSingleNodeTestCase {

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

    private void waitForType(String keyspace, String typeName) throws Exception {
        assertBusy(() -> {
            UntypedResultSet results = process(
                ConsistencyLevel.ONE,
                "SELECT type_name FROM system_schema.types WHERE keyspace_name = ? AND type_name = ?",
                keyspace,
                typeName
            );
            assertThat(results.size(), equalTo(1));
        }, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testNamedUDT() throws Exception {
        final String type = "testudt";
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("user")
                            .field("type", "object")
                            .field("cql_udt_name", "mytype")
                            .field("cql_collection", "singleton")
                            .startObject("properties")
                                .startObject("user_id")
                                    .field("type", "long")
                                .endObject()
                                .startObject("user_email")
                                    .field("type", "keyword")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("message")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject();

        createIndex("twitter", Settings.EMPTY, type, mapping1);
        ensureGreen("twitter");

        assertThat(client().prepareIndex("twitter", type, "1")
                .setSource("{ \"message\": \"hello\", \"user\": { \"user_id\": 500, \"user_email\":\"user@test.com\" } }", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        assertThat(client().prepareGet().setIndex("twitter").setType(type).setId("1")
                .get().isExists(), equalTo(true));

        waitForKeyspace("twitter");
        waitForType("twitter", "mytype");
        UntypedResultSet results = process(ConsistencyLevel.ONE,"SELECT field_names, field_types FROM system_schema.types WHERE keyspace_name = 'twitter' AND type_name = 'mytype'");
        assertThat(results.size(),equalTo(1));
    }

    @Test
    public void testIndexNameTranslation() throws Exception {
        XContentBuilder mapping1 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("attachment")
                            .field("type", "binary")
                        .endObject()
                    .endObject()
                .endObject();

        assertThat(client().admin().indices().preparePutTemplate("test_template")
                .addMapping("_doc", mapping1)
                .setSettings(Settings.builder().put("index.number_of_replicas", 0))
                .setPatterns(java.util.Collections.singletonList("test_index-*"))
                .get().isAcknowledged(), equalTo(true));

        assertThat(client().prepareIndex("test_index-2016.12.29", "_doc", "1")
                .setSource("{ \"user\": { \"user_id\": \"500\", \"user_email\":\"user@test.com\" }, \"message\": \"hello\" }", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        ensureGreen("test_index-2016.12.29");
        client().admin().indices().prepareRefresh("test_index-2016.12.29").get();

        assertThat(client().prepareGet().setIndex("test_index-2016.12.29").setType("_doc").setId("1").get().isExists(), equalTo(true));
        assertThat(client().prepareMultiGet().add("test_index-2016.12.29","_doc","1").get().getResponses().length, equalTo(1));
        assertThat(client().prepareSearch("test_index-2016.12.29").setTypes("_doc").setQuery(QueryBuilders.queryStringQuery("message:hello")).get().getHits().getTotalHits().value, equalTo(1L));
        assertThat(client().prepareDelete().setIndex("test_index-2016.12.29").setType("_doc").setId("1").get().getId(), equalTo("1"));
        assertThat(client().admin().indices().prepareDelete("test_index-2016.12.29").get().isAcknowledged(), equalTo(true));
        assertThat(client().admin().indices().prepareDeleteTemplate("test_template").get().isAcknowledged(), equalTo(true));
    }

    // ES Auto-Discovery fails when Cassandra table has at least one UDT field (#77)
    @Test
    public void testDiscoverUDT() throws Exception {
        final String index = ("test_" + randomAlphaOfLength(8)).toLowerCase(Locale.ROOT);
        createIndex(index);
        ensureGreen(index);
        ensureKeyspace(index);

        process(ConsistencyLevel.ONE, "create type " + index + ".fullname (firstname text, lastname text);");
        process(ConsistencyLevel.ONE, "create table " + index + ".testudt (id uuid, name frozen<fullname>, primary key (id));");
        assertAcked(client().admin().indices().preparePutMapping(index).setType("testudt").setSource(discoverMapping("testudt")).get("120s"));
    }

    // test update nested types
    @Test
    public void testUpdateNestedUDT() throws Exception {
        final String index = ("test_" + randomAlphaOfLength(8)).toLowerCase(Locale.ROOT);
        createIndex(index);
        ensureGreen(index);
        ensureKeyspace(index);

        process(ConsistencyLevel.ONE, "create type " + index + ".fullname (firstname text, lastname text);");
        process(ConsistencyLevel.ONE, "create type " + index + ".info (town text, name list<frozen<fullname>>);");
        process(ConsistencyLevel.ONE, "create table " + index + ".testudt (id text, name list<frozen<info>>, primary key (id));");
        assertAcked(client().admin().indices().preparePutMapping(index).setType("testudt").setSource(discoverMapping("testudt")).get("120s"));
        assertThat(client().prepareIndex(index,"testudt","1").setSource("{\"name\":{\"fullname\": {\"firstname\":\"bob\", \"phone\":33 }}}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    @Test
    public void testExists() throws Exception {
        createIndex("test2");
        ensureGreen("test2");
        assertThat(client().prepareIndex("test2","_doc","1").setSource("{\"top\":\"secret\"}", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        client().admin().indices().prepareRefresh("test2").get();
        assertThat(client().prepareSearch("test2").setTypes("_doc").setQuery(QueryBuilders.existsQuery("top")).get().getHits().getTotalHits().value, equalTo(1L));
    }
}
