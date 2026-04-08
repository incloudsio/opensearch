/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 * OpenSearch 1.3 side-car overlay — MetadataFieldMapper / SimpleMappedFieldType APIs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.elassandra.index.mapper.internal;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Explicit;
import org.opensearch.common.Nullable;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.opensearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SimpleMappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Mapper for the _token field (Cassandra partition token).
 */
public class TokenFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_token";
    public static final String CONTENT_TYPE = "_token";

    private static TokenFieldMapper toType(FieldMapper in) {
        return (TokenFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled = MetadataFieldMapper.updateableBoolParam("enabled", m -> toType(m).enabledState, true);

        public Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.singletonList(enabled);
        }

        @Override
        public TokenFieldMapper build(BuilderContext context) {
            return new TokenFieldMapper(enabled.getValue());
        }
    }

    public static final MetadataFieldMapper.TypeParser PARSER = new MetadataFieldMapper.ConfigurableTypeParser(
        c -> new TokenFieldMapper(new Explicit<>(true, false)),
        c -> new Builder()
    );

    /** Exposed for {@link MetadataFieldMapper#updateableBoolParam} initializer. */
    private final Explicit<Boolean> enabledState;

    static final class TokenFieldType extends SimpleMappedFieldType {

        private static final TokenFieldType INSTANCE = new TokenFieldType();

        private TokenFieldType() {
            super(NAME, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        private long parse(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
                }
                if (doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Long.parseLong(value.toString());
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            long v = parse(value);
            return LongPoint.newExactQuery(name(), v);
        }

        @Override
        public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
            long[] v = new long[values.size()];
            for (int i = 0; i < values.size(); ++i) {
                v[i] = parse(values.get(i));
            }
            return LongPoint.newSetQuery(name(), v);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = parse(lowerTerm);
                if (includeLower == false) {
                    if (l == Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = parse(upperTerm);
                if (includeUpper == false) {
                    if (u == Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return LongPoint.newRangeQuery(name(), l, u);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, java.util.function.Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.LONG);
        }
    }

    private TokenFieldMapper(Explicit<Boolean> enabledState) {
        super(TokenFieldType.INSTANCE);
        this.enabledState = enabledState;
    }

    public Explicit<Boolean> enabledState() {
        return enabledState;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // Suppress default parse; token is applied in postParse (fork parity).
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.sourceToParse().token() != null) {
            Long token = context.sourceToParse().token();
            context.doc().add(new LongPoint(NAME, token.longValue()));
            context.doc().add(new NumericDocValuesField(NAME, token.longValue()));
        }
    }

    @Override
    public void createField(ParseContext context, Object object, Optional<String> keyName) throws IOException {
        if (object instanceof Long) {
            long token = ((Long) object).longValue();
            context.doc().add(new LongPoint(NAME, token));
            context.doc().add(new NumericDocValuesField(NAME, token));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
