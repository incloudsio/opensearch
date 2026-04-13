/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
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

package org.elassandra.index;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ElassandraDaemon;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PagingState.RowMark;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.Logger;
import org.elassandra.cluster.DocPrimaryKey;
import org.elassandra.cluster.QueryManager;
import org.elassandra.cluster.SchemaManager;
import org.elassandra.cluster.routing.AbstractSearchStrategy;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ParsingException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationMetaDataBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.fetch.CqlFetchPhase;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * CQL query processor executing Elasticsearch query.
 */
public class ElasticQueryHandler extends QueryProcessor {

    private static final Logger logger = Loggers.getLogger(ElasticQueryHandler.class);

    public static final String SELECTION = "_selection";

    public ElasticQueryHandler() {
        super();
    }

    @Override
    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
        throws RequestExecutionException, RequestValidationException {
        ClientState clientState = queryState.getClientState();
        statement.authorize(clientState);
        statement.validate(clientState);

        if (statement instanceof SelectStatement) {
            SelectStatement select = (SelectStatement) statement;
            if (!select.getSelection().isAggregate()) {
                String elasticQuery = null;
                String elasticOptions = null;
                for (Expression expr : select.getRowFilter(options).getExpressions()) {
                    if (expr.column().name.bytes.equals(ElasticSecondaryIndex.ES_QUERY_BYTE_BUFFER)) {
                        elasticQuery = UTF8Type.instance.getString(expr.getIndexValue());
                        if (elasticOptions != null)
                            break;
                    } else if (expr.column().name.bytes.equals(ElasticSecondaryIndex.ES_OPTIONS_BYTE_BUFFER)) {
                        elasticOptions = UTF8Type.instance.getString(expr.getIndexValue());
                        if (elasticQuery != null)
                            break;
                    }
                }

                if (elasticQuery != null) {
                    ColumnFamilyStore cfs = Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
                    Index index = cfs.indexManager.getIndexByName(ClusterService.buildIndexName(cfs.name));
                    String typeName = select.columnFamily();
                    Map<String, String> esOptions = null;
                    if (elasticOptions != null) {
                        esOptions = new HashMap<>();
                        for (NameValuePair pair : URLEncodedUtils.parse(elasticOptions, Charset.forName("UTF-8")))
                            esOptions.put(pair.getName(), pair.getValue());
                    }
                    if (index instanceof ExtendedElasticSecondaryIndex) {
                        typeName = ((ElasticSecondaryIndex) ((ExtendedElasticSecondaryIndex) index).elasticSecondaryIndex).typeName;
                    } else if (index instanceof ElasticSecondaryIndex) {
                        typeName = ((ElasticSecondaryIndex) index).typeName;
                    }
                    return executeElasticQuery(select, queryState, options, queryStartNanoTime, typeName, elasticQuery, esOptions);
                }
            }
        }
        ResultMessage result = statement.execute(queryState, options, queryStartNanoTime);
        return result == null ? new ResultMessage.Void() : result;
    }

    void handle(QueryState queryState, Client client) {
    }

    ResultMessage executeElasticQuery(SelectStatement select, QueryState queryState, QueryOptions options, long queryStartNanoTime, String typeName, String query, Map<String, String> esOptions) {

        Client client = ElassandraDaemon.instance.node().client();
        final boolean toJson = select.parameters.isJson || (esOptions != null && esOptions.containsKey("json"));
        ThreadContext context = client.threadPool().getThreadContext();
        Map<String, Object> extraParams = null;
        try (ThreadContext.StoredContext stashedContext = context.stashContext()) {
            int limit = select.getLimit(options);
            PagingState paging = options.getPagingState();
            String scrollId = null;
            int remaining = limit;
            if (paging != null) {
                scrollId = ByteBufferUtil.string(paging.partitionKey);
                remaining = paging.remaining;
                if (logger.isDebugEnabled())
                    logger.debug("paging state scrollId={} remaining={}", scrollId, remaining);
            }

            if (Tracing.isTracing()) {
                extraParams = new HashMap<>();
                extraParams.put("_cassandra.trace.session", Tracing.instance.getSessionId().toString());
                extraParams.put("_cassandra.trace.coordinator", FBUtilities.getBroadcastAddressAndPort().address.getHostAddress());
                Tracing.instance.begin("Elasticsearch query", FBUtilities.getBroadcastAddressAndPort().address, Collections.EMPTY_MAP);
            }

            boolean hasAgregation = false;
            SearchResponse resp;
            AggregationMetaDataBuilder aggMetadataBuilder = null;
            if (scrollId == null) {
                SearchSourceBuilder ssb = null;
                try {
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        namedXContentRegistryForQueryParsing(),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        query);
                    ssb = SearchSourceBuilder.fromXContent(parser);
                } catch (ParsingException e) {
                    throw new SyntaxException(e.getMessage());
                }
                String indices = (esOptions != null && esOptions.containsKey("indices")) ? esOptions.get("indices") : select.keyspace();
                SearchRequestBuilder srb = client.prepareSearch(indices)
                    .setSource(ssb);

                AbstractBounds bounds = select.getRestrictions().getPartitionKeyBounds(options);
                if (bounds != null) {
                    Token left = ((PartitionPosition) bounds.left).getToken();
                    Token right = ((PartitionPosition) bounds.right).getToken();
                    // undefined bound is set to minimum.
                    if (!left.isMinimum() || !right.isMinimum()) {
                        Range<Token> range =
                            (!left.isMinimum() && right.isMinimum())
                                ? new Range<>(left, AbstractSearchStrategy.TOKEN_MAX)
                                : new Range<>(left, right);
                        // OpenSearch 1.3: SearchRequest.tokenRanges not merged yet; token routing handled via CqlFetchPhase elsewhere.
                        // srb.setTokenRanges(new HashSet<>(Collections.singletonList(range)));
                        if (logger.isDebugEnabled())
                            logger.debug("tokenRanges={}", range);
                    }
                }
                if (esOptions != null && esOptions.containsKey("preference"))
                    srb.setPreference(esOptions.get("preference"));
                if (esOptions != null && esOptions.containsKey("routing"))
                    srb.setRouting(esOptions.get("routing"));

                hasAgregation = ssb.aggregations() != null;
                if (hasAgregation) {
                    if (logger.isDebugEnabled())
                        logger.debug("type={} es_query={} es_options={} toJson={} size=0 with aggregation",
                            typeName, ssb.toString(), indices, toJson);
                    srb.setSize(0);
                    aggMetadataBuilder = new AggregationMetaDataBuilder(select.keyspace(), "aggs", toJson);
                    aggMetadataBuilder.build("", ssb.aggregations(), select.getSelection());
                } else {
                    if (extraParams == null)
                        extraParams = new HashMap<>();
                    context.putTransient(SELECTION, select.getSelection());
                    extraParams.put(CqlFetchPhase.PROJECTION, select.getSelection().toCQLString());
                    if (toJson)
                        extraParams.put("_json", "true");

                    if (options.getPageSize() > 0 && (limit > options.getPageSize())) {
                        if (logger.isDebugEnabled())
                            logger.debug("type={} es_query={} es_options={} toJson={} size={} with scrolling",
                                typeName, ssb.toString(), indices, toJson, options.getPageSize());
                        srb.setScroll(new Scroll(new TimeValue(60, TimeUnit.SECONDS)));
                        srb.setSize(options.getPageSize());
                    } else {
                        if (logger.isDebugEnabled())
                            logger.debug("type={} es_query={} es_options={} toJson={} size={} with no scrolling",
                                typeName, ssb.toString(), indices, toJson, limit);
                        srb.setSize(Math.min(limit, 10000)); // default index.max_result_window is 10000
                    }
                }
                handle(queryState, client);
                invokeSetExtraParamsIfPresent(srb, extraParams);
                resp = srb.get();
                scrollId = resp.getScrollId();
            } else {
                SearchScrollRequestBuilder ssrb = client.prepareSearchScroll(scrollId);
                ssrb.setScroll("1m"); // timeout for the next scroll fetch
                handle(queryState, client);
                invokeSetExtraParamsIfPresent(ssrb, extraParams);
                resp = ssrb.get();
                scrollId = resp.getScrollId(); // only the most recently received _scroll_id should be used
            }

            ResultSet.ResultMetadata resultMetadata = null;
            List<List<ByteBuffer>> rows = new LinkedList<>();
            if (hasAgregation) {
                // add aggregation results
                flattenAggregation(aggMetadataBuilder, 0, "", resp.getAggregations(), rows);

                if (select.getSelection().isWildcard()) {
                    resultMetadata = new ResultSet.ResultMetadata(aggMetadataBuilder.getColumns());
                } else {
                    List<ColumnSpecification> columns = aggMetadataBuilder.getColumns();
                    List<ColumnSpecification> projectionColumns = new ArrayList<>(aggMetadataBuilder.getColumns().size());
                    int i = 0;
                    for (ColumnMetadata cd : select.getSelection().getColumns()) {
                        if (i < columns.size() && !cd.type.isValueCompatibleWith(columns.get(i).type)) {
                            logger.warn("Aggregation column [" + columns.get(i).name.toString() + "] of type [" +
                                columns.get(i).type + "] is not compatible with projection term [" + cd.name.toCQLString() + "] of type [" + cd.type + "]");
                            throw new InvalidRequestException("Aggregation column " + columns.get(i).name.toString() +
                                " of type " + columns.get(i).type + " is not compatible with projection term " + cd.name.toCQLString());
                        }
                        projectionColumns.add(cd);
                        i++;
                    }
                    resultMetadata = new ResultSet.ResultMetadata(projectionColumns);
                }
            } else {
                // add row results
                Map<Boolean, QueryHandler.Prepared> projectionStatements = null;
                if (logger.isDebugEnabled())
                    logger.debug("scrollId={} hits={}", scrollId, resp.getHits().getHits().length);
                for (SearchHit hit : resp.getHits().getHits()) {
                    List<ByteBuffer> rowVals = searchHitByteBufferValues(hit);
                    if (rowVals == null) {
                        if (projectionStatements == null) {
                            projectionStatements = new HashMap<>();
                        }
                        rowVals = fetchHitByteBufferValues(select, typeName, hit, toJson, projectionStatements);
                    }
                    if (rowVals != null) {
                        rows.add(rowVals);
                    }
                }
                resultMetadata = select.getResultMetadata().copy();
                if (scrollId != null) {
                    // paging management
                    if (remaining != DataLimits.NO_LIMIT)
                        remaining -= rows.size();
                    if (resp.getHits().getHits().length == 0)
                        remaining = 0;

                    if ((options.getPageSize() > 0 && rows.size() < options.getPageSize()) || remaining <= 0) {
                        client.prepareClearScroll().addScrollId(scrollId).get();
                        if (logger.isDebugEnabled())
                            logger.debug("Clear scrollId={}", scrollId);
                        resultMetadata.setHasMorePages(null);
                    } else {
                        resultMetadata.setHasMorePages(new PagingState(
                            ByteBufferUtil.bytes(scrollId, Charset.forName("UTF-8")), (RowMark) null, remaining, remaining));
                        if (logger.isDebugEnabled())
                            logger.debug("new paging state scrollId={} remaining={}", scrollId, remaining);
                    }
                }
            }

            ResultMessage.Rows messageRows = new ResultMessage.Rows(new ResultSet(resultMetadata, rows));
            // see https://docs.datastax.com/en/developer/java-driver/3.2/manual/custom_payloads/
            if (options.getProtocolVersion().isGreaterOrEqualTo(ProtocolVersion.V4)) {
                Map<String, ByteBuffer> customPayload = new HashMap<String, ByteBuffer>();
                customPayload.put("_shards.successful", ByteBufferUtil.bytes(resp.getSuccessfulShards()));
                customPayload.put("_shards.skipped", ByteBufferUtil.bytes(resp.getSkippedShards()));
                customPayload.put("_shards.failed", ByteBufferUtil.bytes(resp.getFailedShards()));
                customPayload.put("_shards.total", ByteBufferUtil.bytes(resp.getTotalShards()));
                customPayload.put("hits.total", ByteBufferUtil.bytes(resp.getHits().getTotalHits().value));
                customPayload.put("hits.max_score", ByteBufferUtil.bytes(resp.getHits().getMaxScore()));
                if (logger.isDebugEnabled())
                    logger.debug("Add custom payload, _shards.successful={}, _shards.skipped={}, _shards.failed={}, _shards.total={}, hits.total={}, hits.max_score={}",
                        resp.getSuccessfulShards(),
                        resp.getSkippedShards(),
                        resp.getFailedShards(),
                        resp.getTotalShards(),
                        resp.getHits().getTotalHits().value,
                        resp.getHits().getMaxScore());
                messageRows.setCustomPayload(customPayload);
            } else {
                if (logger.isDebugEnabled())
                    logger.debug("Cannot add payload, ProtocolVersion={}", options.getProtocolVersion());
            }
            if (Tracing.isTracing())
                Tracing.instance.stopSession();
            return messageRows;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Set element at a position in the list
    void setElement(List<ByteBuffer> l, int index, ByteBuffer element) {
        l.set(index, element);
    }


    List<ByteBuffer> getRow(final AggregationMetaDataBuilder amdb, long level, List<List<ByteBuffer>> rows) {
        List<ByteBuffer> row = rows.size() > 0 ? rows.get(rows.size() - 1) : null;
        if (level == 0) {
            row = Arrays.asList(new ByteBuffer[amdb.size()]);
            rows.add(row);
        }
        return row;
    }

    List<ByteBuffer> getRowForBucket(final AggregationMetaDataBuilder amdb, long level, int index, boolean firstBucket, List<List<ByteBuffer>> rows) {
        List<ByteBuffer> row = getRow(amdb, level, rows);
        if (!firstBucket && level > 0) {
            // duplicate left part of the row to fill right part for buckets.
            List<ByteBuffer> row2 = Arrays.asList(new ByteBuffer[amdb.size()]);
            for (int i = 0; i < index; i++) {
                if (row.get(i) != null)
                    row2.set(i, row.get(i).duplicate());
            }
            rows.add(row2);
            row = row2;
        }
        return row;
    }

    // flatten tree results to a table of rows.
    void flattenAggregation(final AggregationMetaDataBuilder amdb, final long level, String prefix, final Aggregations aggregations, List<List<ByteBuffer>> rows) throws IOException {
        List<ByteBuffer> row; // current filled row
        for (Aggregation agg : aggregations) {
            String type = agg.getType();
            String baseName = prefix + agg.getName() + ".";

            if (amdb.toJson()) {
                switch (type) {
                    case DoubleTerms.NAME:
                    case LongTerms.NAME:
                    case StringTerms.NAME: {
                        for (Terms.Bucket termBucket : ((Terms) agg).getBuckets()) {
                            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                            xContentBuilder.startObject();
                            termBucket.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                            xContentBuilder.endObject();
                            setElement(getRow(amdb, level, rows), amdb.getColumn(agg.getName()), ByteBufferUtil.bytes(BytesReference.bytes(xContentBuilder).utf8ToString()));
                        }
                    }
                    break;
                    case HistogramAggregationBuilder.NAME: {
                        for (InternalHistogram.Bucket histoBucket : ((InternalHistogram) agg).getBuckets()) {
                            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                            if (histoBucket.getKeyed())
                                xContentBuilder.startObject();
                            histoBucket.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                            if (histoBucket.getKeyed())
                                xContentBuilder.endObject();
                            setElement(getRow(amdb, level, rows), amdb.getColumn(agg.getName()), ByteBufferUtil.bytes(BytesReference.bytes(xContentBuilder).utf8ToString()));
                        }
                    }
                    break;
                    case DateHistogramAggregationBuilder.NAME: {
                        for (InternalDateHistogram.Bucket histoBucket : ((InternalDateHistogram) agg).getBuckets()) {
                            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                            if (histoBucket.getKeyed())
                                xContentBuilder.startObject();
                            histoBucket.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                            if (histoBucket.getKeyed())
                                xContentBuilder.endObject();
                            setElement(getRow(amdb, level, rows), amdb.getColumn(agg.getName()), ByteBufferUtil.bytes(BytesReference.bytes(xContentBuilder).utf8ToString()));
                        }
                    }
                    break;
                    case InternalTDigestPercentiles.NAME:
                    case InternalHDRPercentiles.NAME:
                    case Percentiles.TYPE_NAME: {
                        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                        xContentBuilder.startObject();
                        xContentBuilder.startObject("values");
                        for (Percentile percentile : (Percentiles) agg)
                            xContentBuilder.field(Double.toString(percentile.getPercent()), percentile.getValue());
                        xContentBuilder.endObject();
                        xContentBuilder.endObject();
                        setElement(getRow(amdb, level, rows), amdb.getColumn(agg.getName()), ByteBufferUtil.bytes(BytesReference.bytes(xContentBuilder).utf8ToString()));
                    }
                    break;
                    case SumAggregationBuilder.NAME:
                        setElement(getRow(amdb, level, rows), amdb.getColumn(baseName + "sum"), ByteBufferUtil.bytes((double) ((Sum) agg).getValue()));
                        break;
                    case AvgAggregationBuilder.NAME:
                        setElement(getRow(amdb, level, rows), amdb.getColumn(baseName + "avg"), ByteBufferUtil.bytes((double) ((Avg) agg).getValue()));
                        break;
                    case MinAggregationBuilder.NAME:
                        setElement(getRow(amdb, level, rows), amdb.getColumn(baseName + "min"), ByteBufferUtil.bytes((double) ((Min) agg).getValue()));
                        break;
                    case MaxAggregationBuilder.NAME:
                        setElement(getRow(amdb, level, rows), amdb.getColumn(baseName + "max"), ByteBufferUtil.bytes((double) ((Max) agg).getValue()));
                        break;
                    case StatsAggregationBuilder.NAME: {
                        Stats stats = (Stats) agg;
                        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                        xContentBuilder.startObject();
                        xContentBuilder.field("count", stats.getCount());
                        xContentBuilder.field("min", stats.getMin());
                        xContentBuilder.field("max", stats.getMax());
                        xContentBuilder.field("avg", stats.getAvg());
                        xContentBuilder.field("sum", stats.getSum());
                        xContentBuilder.endObject();
                        setElement(getRow(amdb, level, rows), amdb.getColumn(agg.getName()), ByteBufferUtil.bytes(BytesReference.bytes(xContentBuilder).utf8ToString()));
                    }
                    break;
                    default:
                        logger.error("unsupported aggregation type=[{}] name=[{}]", type, agg.getName());
                        throw new IllegalArgumentException("unsupported aggregation type=[" + type + "] name=[" + agg.getName() + "]");
                }
            } else {
                switch (type) {
                    case StringTerms.NAME: {
                        int keyIdx = amdb.getColumn(baseName + "key");
                        int cntIdx = amdb.getColumn(baseName + "count");
                        boolean fistBucket = true;
                        for (Terms.Bucket termBucket : ((Terms) agg).getBuckets()) {
                            row = getRowForBucket(amdb, level, keyIdx, fistBucket, rows);
                            setElement(row, keyIdx, ByteBufferUtil.bytes(termBucket.getKeyAsString()));
                            setElement(row, cntIdx, ByteBufferUtil.bytes(termBucket.getDocCount()));
                            if (termBucket.getAggregations().iterator().hasNext())
                                flattenAggregation(amdb, level + 1, baseName, termBucket.getAggregations(), rows);
                            fistBucket = false;
                        }
                    }
                    break;
                    case LongTerms.NAME: {
                        int keyIdx = amdb.getColumn(baseName + "key");
                        int cntIdx = amdb.getColumn(baseName + "count");
                        amdb.setColumnType(keyIdx, baseName + "key", LongType.instance);
                        boolean fistBucket = true;
                        for (Terms.Bucket termBucket : ((Terms) agg).getBuckets()) {
                            row = getRowForBucket(amdb, level, keyIdx, fistBucket, rows);
                            setElement(row, keyIdx, ByteBufferUtil.bytes((long) termBucket.getKeyAsNumber()));
                            setElement(row, cntIdx, ByteBufferUtil.bytes(termBucket.getDocCount()));
                            if (termBucket.getAggregations().iterator().hasNext())
                                flattenAggregation(amdb, level + 1, baseName, termBucket.getAggregations(), rows);
                            fistBucket = false;
                        }
                    }
                    break;
                    case DoubleTerms.NAME: {
                        int keyIdx = amdb.getColumn(baseName + "key");
                        int cntIdx = amdb.getColumn(baseName + "count");
                        amdb.setColumnType(keyIdx, baseName + "key", DoubleType.instance);
                        boolean fistBucket = true;
                        for (Terms.Bucket termBucket : ((Terms) agg).getBuckets()) {
                            row = getRowForBucket(amdb, level, keyIdx, fistBucket, rows);
                            setElement(row, keyIdx, ByteBufferUtil.bytes((double) termBucket.getKeyAsNumber()));
                            setElement(row, cntIdx, ByteBufferUtil.bytes(termBucket.getDocCount()));
                            if (termBucket.getAggregations().iterator().hasNext())
                                flattenAggregation(amdb, level + 1, baseName, termBucket.getAggregations(), rows);
                            fistBucket = false;
                        }
                    }
                    break;
                    case DateHistogramAggregationBuilder.NAME: {
                        int keyIdx = amdb.getColumn(baseName + "key");
                        int cntIdx = amdb.getColumn(baseName + "count");
                        boolean fistBucket = true;
                        for (InternalDateHistogram.Bucket histoBucket : ((InternalDateHistogram) agg).getBuckets()) {
                            row = getRowForBucket(amdb, level, keyIdx, fistBucket, rows);
                            setElement(row, keyIdx, TimestampType.instance.getSerializer().serialize(dateHistogramKeyToDate(histoBucket.getKey())));
                            setElement(row, cntIdx, ByteBufferUtil.bytes(histoBucket.getDocCount()));
                            if (histoBucket.getAggregations().iterator().hasNext())
                                flattenAggregation(amdb, level + 1, baseName, histoBucket.getAggregations(), rows);
                            fistBucket = false;
                        }
                    }
                    break;
                    case HistogramAggregationBuilder.NAME: {
                        int keyIdx = amdb.getColumn(baseName + "key");
                        int cntIdx = amdb.getColumn(baseName + "count");
                        boolean fistBucket = true;
                        for (InternalHistogram.Bucket histoBucket : ((InternalHistogram) agg).getBuckets()) {
                            row = getRowForBucket(amdb, level, keyIdx, fistBucket, rows);
                            setElement(row, keyIdx, ByteBufferUtil.bytes((double) histoBucket.getKey()));
                            setElement(row, cntIdx, ByteBufferUtil.bytes(histoBucket.getDocCount()));
                            if (histoBucket.getAggregations().iterator().hasNext())
                                flattenAggregation(amdb, level + 1, baseName, histoBucket.getAggregations(), rows);
                            fistBucket = false;
                        }
                    }
                    break;
                    case SumAggregationBuilder.NAME: {
                        Sum sum = (Sum) agg;
                        row = getRow(amdb, level, rows);
                        setElement(row, amdb.getColumn(baseName + "sum"), ByteBufferUtil.bytes((double) sum.getValue()));
                    }
                    break;
                    case AvgAggregationBuilder.NAME: {
                        Avg avg = (Avg) agg;
                        row = getRow(amdb, level, rows);
                        setElement(row, amdb.getColumn(baseName + "avg"), ByteBufferUtil.bytes((double) avg.getValue()));
                    }
                    break;
                    case MinAggregationBuilder.NAME: {
                        Min min = (Min) agg;
                        row = getRow(amdb, level, rows);
                        setElement(row, amdb.getColumn(baseName + "min"), ByteBufferUtil.bytes((double) min.getValue()));
                    }
                    break;
                    case MaxAggregationBuilder.NAME: {
                        Max max = (Max) agg;
                        row = getRow(amdb, level, rows);
                        setElement(row, amdb.getColumn(baseName + "max"), ByteBufferUtil.bytes((double) max.getValue()));
                    }
                    break;
                    case "simple_value": {
                        InternalSimpleValue simpleValue = (InternalSimpleValue) agg;
                        int valIdx = amdb.getColumn(simpleValue.getName());
                        row = getRow(amdb, level, rows);
                        setElement(row, valIdx, ByteBufferUtil.bytes(simpleValue.getValue()));
                    }
                    break;
                    default:
                        logger.error("unsupported aggregation type=[{}] name=[{}]", type, agg.getName());
                        throw new IllegalArgumentException("unsupported aggregation type=[" + type + "] name=[" + agg.getName() + "]");
                }
            }
        }
    }

    private static NamedXContentRegistry namedXContentRegistryForQueryParsing() {
        try {
            if (ElassandraDaemon.instance != null && ElassandraDaemon.instance.node() != null) {
                NamedXContentRegistry registry = ElassandraDaemon.instance.node().injector().getInstance(NamedXContentRegistry.class);
                if (registry != null) {
                    return registry;
                }
            }
        } catch (Throwable ignored) {
            // side-car compile stub / Node API without registry
        }
        return NamedXContentRegistry.EMPTY;
    }

    @SuppressWarnings("unchecked")
    private static void invokeSetExtraParamsIfPresent(Object builder, Map<String, Object> extraParams) {
        if (extraParams == null) {
            return;
        }
        try {
            Method m = builder.getClass().getMethod("setExtraParams", Map.class);
            m.invoke(builder, extraParams);
        } catch (NoSuchMethodException ignored) {
            // OpenSearch 1.3 removed SearchRequestBuilder#setExtraParams; CQL projection transport is wired elsewhere until ported.
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<ByteBuffer> searchHitByteBufferValues(SearchHit hit) {
        try {
            Method m = hit.getClass().getMethod("getValues");
            return (List<ByteBuffer>) m.invoke(hit);
        } catch (NoSuchMethodException e) {
            return null;
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    private static List<ByteBuffer> fetchHitByteBufferValues(
        SelectStatement select,
        String typeName,
        SearchHit hit,
        boolean toJson,
        Map<Boolean, QueryHandler.Prepared> projectionStatements
    ) {
        try {
            ClusterService clusterService = ElassandraDaemon.instance.node().injector().getInstance(ClusterService.class);
            QueryManager queryManager = new QueryManager(ElassandraDaemon.instance.node().settings(), clusterService);
            IndexMetadata indexMetaData = clusterService.state().metadata().index(select.keyspace());
            if (indexMetaData == null) {
                return null;
            }
            IndexService indexService = clusterService.getIndicesService().indexService(indexMetaData.getIndex());
            if (indexService == null) {
                return null;
            }
            IndexShard indexShard = indexService.getShardOrNull(0);
            if (indexShard == null) {
                return null;
            }

            DocPrimaryKey docPk = queryManager.parseElasticId(indexService.mapperService().keyspace(), typeName, hit.getId());
            QueryHandler.Prepared prepared = projectionStatements.get(docPk.isStaticDocument);
            if (prepared == null) {
                prepared = QueryProcessor.prepareInternal(
                    buildProjectionFetchQuery(indexShard, typeName, select.getSelection().toCQLString(), docPk.isStaticDocument, toJson)
                );
                projectionStatements.put(docPk.isStaticDocument, prepared);
            }

            ResultMessage result = prepared.statement.executeLocally(
                new QueryState(ClientState.forInternalCalls()),
                QueryOptions.forInternalCalls(org.apache.cassandra.db.ConsistencyLevel.ONE, docPk.serialize(prepared))
            );
            if (result instanceof ResultMessage.Rows) {
                return firstResultRowBuffers((ResultMessage.Rows) result);
            }
        } catch (Exception e) {
            logger.warn("Fallback CQL projection fetch failed for hit id={} type={}", hit.getId(), typeName, e);
        }
        return null;
    }

    private static String buildProjectionFetchQuery(
        IndexShard indexShard,
        String typeName,
        String projection,
        boolean forStaticDocument,
        boolean isJson
    ) throws IOException {
        org.opensearch.index.mapper.DocumentMapper docMapper = indexShard.mapperService().documentMapper(typeName);
        String cfName = SchemaManager.typeToCfName(indexShard.mapperService().keyspace(), typeName);
        org.opensearch.index.mapper.DocumentMapper.CqlFragments cqlFragment = docMapper.getCqlFragments();
        return new StringBuilder()
            .append("SELECT ")
            .append(isJson ? "JSON " : "")
            .append(projection)
            .append(" FROM \"").append(indexShard.mapperService().keyspace()).append("\".\"").append(cfName).append("\"")
            .append(" WHERE ")
            .append(forStaticDocument ? cqlFragment.ptWhere : cqlFragment.pkWhere)
            .append(" LIMIT 1")
            .toString();
    }

    private static List<ByteBuffer> firstResultRowBuffers(ResultMessage.Rows result) {
        return result.result == null || result.result.isEmpty() ? null : result.result.firstRow();
    }

    private static Date dateHistogramKeyToDate(Object key) {
        if (key instanceof DateTime) {
            return ((DateTime) key).toDate();
        }
        if (key instanceof ZonedDateTime) {
            return Date.from(((ZonedDateTime) key).toInstant());
        }
        if (key instanceof Date) {
            return (Date) key;
        }
        throw new IllegalArgumentException("Unsupported date histogram key type: " + (key == null ? "null" : key.getClass().getName()));
    }
}
