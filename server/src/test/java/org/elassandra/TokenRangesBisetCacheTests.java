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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author vroyer
 *
 */
public class TokenRangesBisetCacheTests extends OpenSearchSingleNodeTestCase {
    static long N = 11000; // start query caching at 10k

    @Test
    @Ignore("Token range bitset cache behavior is not currently compatible with the OpenSearch 1.3 sidecar query execution path.")
    public void tokenBitsetTest() throws Exception {
        process(
            ConsistencyLevel.ONE,
            "CREATE KEYSPACE IF NOT EXISTS test WITH replication={ 'class':'NetworkTopologyStrategy', '"
                + DatabaseDescriptor.getLocalDataCenter()
                + "':'1' }"
        );
        process(ConsistencyLevel.ONE,"CREATE TABLE IF NOT EXISTS test.t1 ( a int,b bigint, primary key (a) )");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("t1").field("discover", ".*").endObject().endObject();
        createIndex("test", Settings.builder()
                .put("index.queries.cache.enabled",true)
                .build(),"t1", mapping);
        ensureGreen("test");

        for(int j=0 ; j < N; j++)
            process(ConsistencyLevel.ONE,"insert into test.t1 (a,b) VALUES (?,?)", j, OpenSearchSingleNodeTestCase.randomLong());

        // ensure we have at least one segment > 10k docs.
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).setFlush(true).get();
        boolean hasOneBigSegment = false;
        for(IndexShardSegments iss : client().admin().indices().prepareSegments("test").get().getIndices().get("test")) {
            for(ShardSegments ss : iss.getShards()) {
                for(Segment seg : ss.getSegments()) {
                    if (seg.getNumDocs() > 10000)
                        hasOneBigSegment = true;
                }
            }
        }
        assertThat(hasOneBigSegment, equalTo(true));

        // force caching after 20 requests.
        long nbHits = 0;
        for(int i=0; i< 30 ; i++) {
            nbHits = client().prepareSearch().setIndices("test").setTypes("t1")
                .setQuery(QueryBuilders.rangeQuery("b").gte(0))
                .get().getHits().getTotalHits().value;
        }

        long upper = client().prepareSearch().setIndices("test").setTypes("t1")
                .setQuery(QueryBuilders.rangeQuery("b").gte(0))
                .get().getHits().getTotalHits().value;
        assertThat(upper, lessThan(nbHits));

        long lower = client().prepareSearch().setIndices("test").setTypes("t1")
                .setQuery(QueryBuilders.rangeQuery("b").gte(0))
                .get().getHits().getTotalHits().value;
        assertThat(lower, lessThan(nbHits));

        assertThat(lower+upper, equalTo(nbHits));
        assertThat(client().prepareSearch().setIndices("test").setTypes("t1").setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits().value, equalTo(N));
    }

}
