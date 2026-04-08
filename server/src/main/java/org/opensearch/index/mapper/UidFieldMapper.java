/*
 * Elassandra side-car: legacy {@code _uid} field (type#id) for secondary indexing / CQL document materialization.
 * OpenSearch 7+ removed {@code _uid} in favor of {@link IdFieldMapper}; this mapper exists for fork parity.
 */
package org.opensearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * Metadata mapper for the legacy {@code _uid} stored field.
 */
public class UidFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_uid";
    public static final String CONTENT_TYPE = "_uid";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new UidFieldMapper());

    public static final class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    static final class UidFieldType extends StringFieldType {
        static final UidFieldType INSTANCE = new UidFieldType();

        private UidFieldType() {
            super(NAME, true, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
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

    private UidFieldMapper() {
        super(UidFieldType.INSTANCE);
    }

    @Override
    public void createField(ParseContext context, Object object, Optional<String> keyName) throws IOException {
        if (object instanceof Uid) {
            Uid uid = (Uid) object;
            String uidString = Uid.createUid(uid.type(), uid.id());
            context.doc().add(new Field(NAME, uidString, Defaults.FIELD_TYPE));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
