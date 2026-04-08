/*
 * OpenSearch 1.3 variant (see scripts/templates/opensearch-sidecar/TokenRangeAggregationBuilder.java).
 */
package org.opensearch.search.aggregations.bucket.range;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.bucket.range.InternalRange.Factory;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class TokenRangeAggregatorFactory extends RangeAggregatorFactory {

    public TokenRangeAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Range[] ranges,
        boolean keyed,
        Factory<?, ?> rangeFactory,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, ranges, keyed, rangeFactory, queryShardContext, parent, subFactoriesBuilder, metadata);
    }
}
