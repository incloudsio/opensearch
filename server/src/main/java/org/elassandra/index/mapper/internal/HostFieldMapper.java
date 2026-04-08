/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * OpenSearch 1.3 side-car overlay — StringFieldType-based metadata mapper (KeywordFieldType is final).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.elassandra.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.common.Explicit;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Mapper for _host (Cassandra node id).
 */
public class HostFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_host";
    public static final String CONTENT_TYPE = "_host";

    public static class Defaults {
        public static final org.apache.lucene.document.FieldType FIELD_TYPE = new org.apache.lucene.document.FieldType();
        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    private static HostFieldMapper toType(FieldMapper in) {
        return (HostFieldMapper) in;
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
        public HostFieldMapper build(BuilderContext context) {
            return new HostFieldMapper(enabled.getValue());
        }
    }

    public static final MetadataFieldMapper.TypeParser PARSER = new MetadataFieldMapper.ConfigurableTypeParser(
        c -> new HostFieldMapper(new Explicit<>(true, false)),
        c -> new Builder()
    );

    private final Explicit<Boolean> enabledState;

    static final class HostFieldType extends StringFieldType {

        static final HostFieldType INSTANCE = new HostFieldType();

        private HostFieldType() {
            super(NAME, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }

    private HostFieldMapper(Explicit<Boolean> enabledState) {
        super(HostFieldType.INSTANCE);
        this.enabledState = enabledState;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Populated via createField from ElasticSecondaryIndex (fork parity).
    }

    @Override
    public void createField(ParseContext context, Object object, Optional<String> keyName) throws IOException {
        String host = (String) object;
        if (host != null && fieldType().isSearchable()) {
            context.doc().add(new Field(fieldType().name(), host, Defaults.FIELD_TYPE));
            createFieldNamesField(context);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
