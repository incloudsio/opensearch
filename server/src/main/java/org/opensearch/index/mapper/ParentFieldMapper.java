/*
 * Elassandra side-car stub: ES 6.x parent/child metadata field; removed in stock OpenSearch 7+.
 * Compile-only placeholder — routing-based relations replaced join fields in upstream.
 */
package org.opensearch.index.mapper;

import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;

/**
 * Legacy _parent field mapper (inactive stub).
 */
public class ParentFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_parent";
    public static final String CONTENT_TYPE = "_parent";
    /** Elassandra CQL mapping key for parent PK columns (fork parity). */
    public static final String CQL_PARENT_PK = "cql_parent_pk";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new ParentFieldMapper());

    static final class ParentFieldType extends StringFieldType {
        static final ParentFieldType INSTANCE = new ParentFieldType();

        private ParentFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
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

    private ParentFieldMapper() {
        super(ParentFieldType.INSTANCE);
    }

    public boolean active() {
        return false;
    }

    public String type() {
        return "";
    }

    /** Elassandra: comma-separated CQL PK column names for parent routing (stub returns null). */
    public String pkColumns() {
        return null;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
