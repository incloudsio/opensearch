package org.opensearch.index.mapper;

import java.io.IOException;
import java.util.List;

/**
 * Bridges Elassandra code in other packages to {@link DocumentParser} static helpers that are
 * package-private in OpenSearch 1.x (cannot be called from {@code org.elassandra} directly).
 */
public final class DocumentParserCompat {

    private DocumentParserCompat() {}

    public static void createCopyFields(ParseContext context, List<String> copyToFields, Object value) throws IOException {
        DocumentParser.createCopyFields(context, copyToFields, value);
    }

    public static ParseContext nestedContext(ParseContext context, ObjectMapper mapper) throws IOException {
        return DocumentParser.nestedContext(context, mapper);
    }

    public static ObjectMapper.Dynamic dynamicOrDefault(ObjectMapper parentMapper, ParseContext context) throws IOException {
        return DocumentParser.dynamicOrDefault(parentMapper, context);
    }

    public static void nested(ParseContext context, ObjectMapper.Nested nested) throws IOException {
        DocumentParser.nested(context, nested);
    }
}
