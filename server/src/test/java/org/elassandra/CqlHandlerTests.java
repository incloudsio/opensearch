package org.elassandra;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.elassandra.index.ElasticIncomingPayload;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakScope(Scope.NONE)
@ThreadLeakZombies(Consequence.CONTINUE)
public class CqlHandlerTests extends OpenSearchSingleNodeTestCase {

    public CqlHandlerTests() {
        super();
    }

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

    @SuppressForbidden(reason="test")
    @Test
    public void testSearch() throws IOException, InterruptedException {
        final String index = "cql_handler_search_test";
        final String table = "foo";
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("foo").field("type", "keyword").field("cql_collection", "singleton").endObject()
                        .startObject("es_query").field("type", "keyword").field("cql_collection", "singleton").field("index","false").endObject()
            .endObject()
                .endObject();
        try {
            ensureKeyspace(index);
        } catch (Exception e) {
            throw new IOException(e);
        }
        process(ConsistencyLevel.ONE, "CREATE TABLE " + index + "." + table + " (\"_id\" text PRIMARY KEY, foo text, es_query text)");
        process(
            ConsistencyLevel.ONE,
            "CREATE CUSTOM INDEX elastic_foo_idx ON " + index + "." + table + " () USING 'org.elassandra.index.ExtendedElasticSecondaryIndex';"
        );
        createIndex(index);
        ensureGreen(index);
        assertAcked(client().admin().indices().preparePutMapping(index).setType(table).setSource(mapping).get());

        for (int i = 0; i < 100; i++) {
            process(ConsistencyLevel.ONE, "INSERT INTO " + index + "." + table + " (\"_id\", foo) VALUES (?, ?)", Integer.toString(i), "bar");
        }
        client().admin().indices().prepareRefresh(index).get();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        String esQuery = sourceBuilder.toString(ToXContent.EMPTY_PARAMS);

        try {
            assertBusy(() -> {
                try {
                    client().admin().indices().prepareRefresh(index).get();
                    UntypedResultSet busyRs = process(
                        ConsistencyLevel.ONE,
                        "SELECT * FROM " + index + "." + table + " WHERE es_query=? ALLOW FILTERING",
                        esQuery
                    );
                    assertThat(busyRs.size(), equalTo(100));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            throw new AssertionError(e);
        }

        UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + "." + table + " WHERE es_query=? ALLOW FILTERING", esQuery);
        assertThat(rs.size(), equalTo(100));

        // with function
        Date now = new Date();
        rs = process(
            ConsistencyLevel.ONE,
            "SELECT writetime(foo), token(\"_id\") FROM " + index + "." + table + " WHERE es_query=? LIMIT 100 ALLOW FILTERING",
            esQuery
        );
        assertThat(rs.size(), equalTo(100));
        UntypedResultSet.Row row = rs.iterator().next();
        assertThat(row.getColumns().size(), equalTo(2));
        assertThat(row.getTimestamp("writetime(foo)"), greaterThan(now));

        // with limit
        rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + "." + table + " WHERE es_query=? LIMIT 50 ALLOW FILTERING", esQuery);
        assertThat(rs.size(), equalTo(50));

        // with limit over index size
        rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + "." + table + " WHERE es_query=? LIMIT 5000 ALLOW FILTERING", esQuery);
        assertThat(rs.size(), equalTo(100));

        // message payload with protocol v4
        Long writetime = new Long(0);
        ByteBuffer buffer = UTF8Type.instance.decompose(esQuery);
        QueryOptions queryOptions = QueryOptions.create(ConsistencyLevel.ONE, Collections.singletonList(buffer), false, 5000, null, null, ProtocolVersion.V4, null);
        QueryState queryState = new QueryState( ClientState.forInternalCalls());
        CQLStatement stmt = QueryProcessor.instance.parse(
            "SELECT * FROM " + index + "." + table + " WHERE es_query=? ALLOW FILTERING",
            queryState,
            queryOptions
        );
        ResultMessage message = ClientState.getCQLQueryHandler().process(stmt, queryState, queryOptions, Collections.emptyMap(), System.nanoTime());
        ElasticIncomingPayload payloadInfo = new ElasticIncomingPayload(message.getCustomPayload());
        assertThat(payloadInfo.hitTotal, equalTo(100L));
        assertThat(payloadInfo.shardSuccessful, equalTo(1));
        assertThat(payloadInfo.shardFailed, equalTo(0));
        assertThat(payloadInfo.shardSkipped, equalTo(0));

        // page size = 75
        queryOptions = QueryOptions.create(ConsistencyLevel.ONE, Collections.singletonList(buffer), false, 75, null, null, ProtocolVersion.V4, null);
        stmt = QueryProcessor.instance.parse(
            "SELECT * FROM " + index + "." + table + " WHERE es_query=? LIMIT 1000 ALLOW FILTERING",
            queryState,
            queryOptions
        );
        message = ClientState.getCQLQueryHandler().process(stmt, queryState, queryOptions, Collections.emptyMap(), System.nanoTime());
        rs = UntypedResultSet.create(((ResultMessage.Rows) message).result);
        assertThat(rs.size(), equalTo(75));
    }

    @SuppressForbidden(reason="test")
    @Test
    public void testCqlAggregation() throws IOException {
        final String index = "cql_handler_iot_test";
        try {
            ensureKeyspace(index);
        } catch (Exception e) {
            throw new IOException(e);
        }
        process(
            ConsistencyLevel.ONE,
            "CREATE TABLE " + index + ".sensor ( name text, ts timestamp, water int, power double, es_query text, es_options text, primary key ((name),ts))"
        );

        // round initial date to a point for stable daily aggregation.
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        int N = 10;
        int P = 5;
        process(
            ConsistencyLevel.ONE,
            "CREATE CUSTOM INDEX elastic_sensor_idx ON " + index + ".sensor () USING 'org.elassandra.index.ExtendedElasticSecondaryIndex';"
        );
        createIndex(index);
        ensureGreen(index);
        assertAcked(client().admin().indices().preparePutMapping(index)
                .setType("sensor")
                .setSource("{ \"sensor\" : { \"discover\" : \".*\" }}", XContentType.JSON)
                .get());
        for (long i = 0; i < 24 * 10; i++) {
            Date ts = new Date(cal.getTime().getTime() + i * 3600 * 1000);
            int water = (int) i % 2;
            double power = (i * P) / 240.0;
            assertThat(
                    client().prepareIndex(index, "sensor", Long.toString(i))
                            .setSource(
                                    XContentFactory.jsonBuilder()
                                            .startObject()
                                            .field("name", "box1")
                                            .field("ts", ts)
                                            .field("water", water)
                                            .field("power", power)
                                            .endObject()
                            )
                            .get()
                            .getResult()
                            .getOp(),
                    equalTo((byte) 0)
            );
        }
        client().admin().indices().prepareRefresh(index).get();

        SumAggregationBuilder aggPower = AggregationBuilders.sum("agg_power").field("power");
        TermsAggregationBuilder aggWater = AggregationBuilders.terms("agg_water").field("water").subAggregation(aggPower);
        DateHistogramAggregationBuilder dailyAgg = AggregationBuilders.dateHistogram("daily_agg")
                .field("ts")
                .dateHistogramInterval(DateHistogramInterval.DAY)
                .minDocCount(0)
                .subAggregation(aggWater);
        HistogramAggregationBuilder histoAgg = AggregationBuilders.histogram("power_histo")
            .field("power")
            .interval(1.0)
            .minDocCount(0);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(new MatchAllQueryBuilder())
                .aggregation(dailyAgg)
                .aggregation(histoAgg);
        String esQuery = sourceBuilder.toString(ToXContent.EMPTY_PARAMS);
        try {
            assertBusy(() -> {
                try {
                    client().admin().indices().prepareRefresh(index).get();
                    org.opensearch.action.search.SearchResponse directAggregationResponse =
                            client().prepareSearch(index).setSource(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0)).get();
                    assertThat(directAggregationResponse.getHits().getTotalHits().value, equalTo(240L));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        // default limit is 10
        UntypedResultSet rs = process(ConsistencyLevel.ONE, "SELECT * FROM " + index + ".sensor WHERE es_query=?", esQuery);
        allowedWarnings(
                "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release",
                "[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
        assertThat(rs.size(), equalTo(N*2 + P));
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

}
