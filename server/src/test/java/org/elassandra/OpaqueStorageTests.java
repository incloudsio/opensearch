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

import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test opaque storage.
 * @author vroyer
 *
 */
//gradle :server:test -Dtests.seed=65E2CF27F286CC89 -Dtests.class=org.elassandra.OpaqueStorageTests -Dtests.security.manager=false -Dtests.locale=en-PH -Dtests.timezone=America/Coral_Harbour
public class OpaqueStorageTests extends OpenSearchSingleNodeTestCase {

    @Test
    public void testOpaqueStorageInsertOnly() throws Exception {
        testOpaqueStorage(true);
    }

    @Test
    public void testOpaqueStorageNotInsertOnly() throws Exception {
        testOpaqueStorage(false);
    }

    public void testOpaqueStorage(boolean insertOnly) throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder()
                .put("index.index_opaque_storage", true)
                .put("index.index_insert_only", insertOnly)));
        ensureGreen("test");

        assertThat(client().prepareIndex("test", "mytype","1").setSource("{ \"foo\":\"bar\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        SearchResponse resp = client().prepareSearch().setIndices("test").setQuery(QueryBuilders.matchAllQuery()).get();
        assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
        assertThat(resp.getHits().getHits()[0].getSourceAsMap().get("foo"), equalTo("bar"));

        assertThat(client().prepareIndex("test", "mytype","2").setSource("{ \"foo\":\"bar\",\"big\":\"bang\" }", XContentType.JSON).get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        SearchResponse resp2 = client().prepareSearch().setIndices("test").setQuery(QueryBuilders.termQuery("big", "bang")).get();
        client().admin().indices().prepareRefresh("test");
        assertThat(resp2.getHits().getTotalHits().value, equalTo(1L));
        assertThat(resp2.getHits().getHits()[0].getSourceAsMap().get("big"), equalTo("bang"));

        GetFieldMappingsResponse mappingResp = client().admin().indices().prepareGetFieldMappings("test").addTypes("mytype").setFields("big").get();
        FieldMappingMetadata fmmd = mappingResp.mappings().get("test").get("mytype").get("big");
        Map<String, Object> mapMd = (Map<String, Object>)fmmd.sourceAsMap().get("big");
        assertThat(mapMd.get("type"), equalTo("text"));

        // copy _source in another doc
        Row row = process(ConsistencyLevel.ONE, "SELECT \"_source\" FROM test.mytype WHERE \"_id\" = ?", "2").one();
        process(ConsistencyLevel.ONE, "INSERT INTO test.mytype (\"_id\",\"_source\") VALUES(?,?)", "1", row.getBlob("_source"));

        SearchResponse resp3 = client().prepareSearch().setIndices("test").setQuery(QueryBuilders.termQuery("big", "bang")).get();
        assertThat(resp3.getHits().getTotalHits().value, equalTo(2L));
    }

}
