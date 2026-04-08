/*
 * OpenSearch 1.3 variant: replaces synced Elassandra file after rewrite-elassandra-imports (org.opensearch.*).
 * Kept in scripts/templates so the main tree stays on Elasticsearch 6.8 APIs.
 */
package org.elassandra.search.aggregations.bucket.token;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.opensearch.search.aggregations.bucket.range.RangeAggregatorFactory;
import org.opensearch.search.aggregations.bucket.range.TokenRangeAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class TokenRangeAggregationBuilder extends RangeAggregationBuilder {

    public static final String NAME = "token_range";

    private static final ObjectParser<TokenRangeAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, TokenRangeAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareBoolean(TokenRangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);
        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) {
                agg.addRange(range);
            }
        }, (p, c) -> RangeAggregator.Range.PARSER.parse(p, null), RangeAggregator.RANGES_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser context) throws IOException {
        return PARSER.parse(context, new TokenRangeAggregationBuilder(aggregationName), aggregationName);
    }

    public TokenRangeAggregationBuilder(String name) {
        super(name);
    }

    public TokenRangeAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    protected TokenRangeAggregationBuilder(
        TokenRangeAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TokenRangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public TokenRangeAggregationBuilder addRange(String key, long from, long to) {
        addRange(new Range(key, (double) from, (double) to));
        return this;
    }

    public TokenRangeAggregationBuilder addRange(long from, long to) {
        return addRange(null, from, to);
    }

    public TokenRangeAggregationBuilder addUnboundedTo(String key, long to) {
        addRange(new Range(key, null, (double) to));
        return this;
    }

    public TokenRangeAggregationBuilder addUnboundedTo(long to) {
        return addUnboundedTo(null, to);
    }

    public TokenRangeAggregationBuilder addUnboundedFrom(String key, long from) {
        addRange(new Range(key, (double) from, null));
        return this;
    }

    public TokenRangeAggregationBuilder addUnboundedFrom(long from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected RangeAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        Range[] ranges = processRanges(range -> {
            DocValueFormat parser = config.format();
            assert parser != null;
            Double from = range.getFrom();
            Double to = range.getTo();
            if (range.getFromAsString() != null) {
                from = parser.parseDouble(range.getFromAsString(), false, queryShardContext::nowInMillis);
            }
            if (range.getToAsString() != null) {
                to = parser.parseDouble(range.getToAsString(), false, queryShardContext::nowInMillis);
            }
            return new Range(range.getKey(), from, range.getFromAsString(), to, range.getToAsString());
        });
        if (ranges.length == 0) {
            throw new IllegalArgumentException("No [ranges] specified for the [" + this.getName() + "] aggregation");
        }
        return new TokenRangeAggregatorFactory(
            name,
            config,
            ranges,
            keyed,
            rangeFactory,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected org.opensearch.search.aggregations.support.ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return RangeAggregationBuilder.REGISTRY_KEY;
    }
}
