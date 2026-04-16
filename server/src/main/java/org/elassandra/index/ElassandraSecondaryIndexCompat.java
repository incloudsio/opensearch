/*
 * Helpers for {@link ElasticSecondaryIndex} when compiling against OpenSearch 1.x APIs that diverged
 * from the Elassandra ES 6.8 fork ({@code IndexService#getMetadata}, {@code NumberFieldMapper.NumberType#rangeQuery}, etc.).
 */
package org.elassandra.index;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

/**
 * Reflection-based compatibility shims.
 */
public final class ElassandraSecondaryIndexCompat {

    private ElassandraSecondaryIndexCompat() {}

    public static long indexMetadataVersion(IndexService indexService) {
        try {
            Method gm = IndexService.class.getMethod("getMetadata");
            Object md = gm.invoke(indexService);
            return ((Number) md.getClass().getMethod("getVersion").invoke(md)).longValue();
        } catch (NoSuchMethodException e) {
            try {
                Method gm = IndexService.class.getMethod("getMetaData");
                Object md = gm.invoke(indexService);
                return ((Number) md.getClass().getMethod("getVersion").invoke(md)).longValue();
            } catch (ReflectiveOperationException e2) {
                throw new OpenSearchException("Could not read index metadata version from IndexService", e2);
            }
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("Could not read index metadata version from IndexService", e);
        }
    }

    /** True when a null incoming value should be ignored (no configured null_value on the mapper). */
    public static boolean nullValueUnset(FieldMapper mapper) {
        if (mapper == null) {
            return true;
        }
        MappedFieldType ft = mapper.fieldType();
        try {
            Method m = ft.getClass().getMethod("nullValue");
            return m.invoke(ft) == null;
        } catch (NoSuchMethodException e) {
            return true;
        } catch (ReflectiveOperationException e) {
            return true;
        }
    }

    public static void fieldMapperCreate(FieldMapper mapper, ParseContext ctx, Object value, Optional<String> keyName) throws IOException {
        Object mapperValue = adaptExternalFieldValue(mapper, value);
        try {
            Method m = FieldMapper.class.getMethod("createField", ParseContext.class, Object.class, Optional.class);
            m.invoke(mapper, ctx, mapperValue, keyName);
        } catch (NoSuchMethodException e) {
            try {
                mapper.parse(ctx.createExternalValueContext(mapperValue));
            } catch (IOException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                throw ex;
            }
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) {
                throw (IOException) c;
            }
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new IOException(c);
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    private static Object adaptExternalFieldValue(FieldMapper mapper, Object value) throws IOException {
        if (value == null || mapper == null || mapper.getClass().getName().endsWith("RangeFieldMapper") == false) {
            return value;
        }
        try {
            Method parse = mapper.getClass().getMethod("parse", Object.class);
            return parse.invoke(mapper, value);
        } catch (NoSuchMethodException e) {
            return value;
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) {
                throw (IOException) c;
            }
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new IOException(c);
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    public static void mapperServiceBuildNativeOrUdtMapping(MapperService mapperService, java.util.Map<String, Object> esMapping,
            AbstractType valueType) throws IOException {
        try {
            Method m = MapperService.class.getMethod("buildNativeOrUdtMapping", java.util.Map.class, AbstractType.class);
            m.invoke(mapperService, esMapping, valueType);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException("MapperService.buildNativeOrUdtMapping is not available", e);
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) {
                throw (IOException) c;
            }
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new IOException(c);
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }

    public static Query numberTypeRangeQuery(NumberFieldMapper.NumberType type, String field, Object lowerTerm, Object upperTerm,
            boolean includeLower, boolean includeUpper, boolean hasDocValues) {
        ReflectiveOperationException last = null;
        for (Method m : type.getClass().getMethods()) {
            if (!"rangeQuery".equals(m.getName()) || m.getParameterTypes().length != 6) {
                continue;
            }
            try {
                return (Query) m.invoke(type, field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues);
            } catch (ReflectiveOperationException e) {
                last = e;
            }
        }
        for (Method m : type.getClass().getMethods()) {
            if (!"rangeQuery".equals(m.getName())) {
                continue;
            }
            Class<?>[] p = m.getParameterTypes();
            if (p.length != 7) {
                continue;
            }
            try {
                return (Query) m.invoke(type, field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues, (Object) null);
            } catch (ReflectiveOperationException e) {
                last = e;
            }
        }
        throw new OpenSearchException("NumberType.rangeQuery: no compatible overload on " + type, last);
    }

    public static Query ipFieldTypeTermQuery(MappedFieldType fieldType, Object value) {
        try {
            Method m = fieldType.getClass().getMethod("termQuery", Object.class, QueryShardContext.class);
            return (Query) m.invoke(fieldType, value, (QueryShardContext) null);
        } catch (NoSuchMethodException e) {
            return ipFieldTypeTermQueryOs(fieldType, value);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("IpFieldType.termQuery compatibility", e);
        }
    }

    private static Query ipFieldTypeTermQueryOs(MappedFieldType fieldType, Object value) {
        try {
            Class<?> qsc = Class.forName("org.opensearch.index.query.QueryShardContext");
            Method m = fieldType.getClass().getMethod("termQuery", Object.class, qsc);
            return (Query) m.invoke(fieldType, value, null);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("IpFieldType.termQuery compatibility", e);
        }
    }

    public static Query ipFieldTypeRangeQuery(MappedFieldType fieldType, Object lowerTerm, Object upperTerm, boolean includeLower,
            boolean includeUpper) {
        try {
            Method m = fieldType.getClass().getMethod("rangeQuery", Object.class, Object.class, boolean.class, boolean.class,
                    QueryShardContext.class);
            return (Query) m.invoke(fieldType, lowerTerm, upperTerm, includeLower, includeUpper, (QueryShardContext) null);
        } catch (NoSuchMethodException e) {
            return ipFieldTypeRangeQueryOs(fieldType, lowerTerm, upperTerm, includeLower, includeUpper);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("IpFieldType.rangeQuery compatibility", e);
        }
    }

    private static Query ipFieldTypeRangeQueryOs(MappedFieldType fieldType, Object lowerTerm, Object upperTerm, boolean includeLower,
            boolean includeUpper) {
        try {
            Class<?> qsc = Class.forName("org.opensearch.index.query.QueryShardContext");
            Method m = fieldType.getClass().getMethod("rangeQuery", Object.class, Object.class, boolean.class, boolean.class, qsc);
            return (Query) m.invoke(fieldType, lowerTerm, upperTerm, includeLower, includeUpper, null);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("IpFieldType.rangeQuery compatibility", e);
        }
    }

    public static Query booleanFieldTypeRangeQuery(MappedFieldType fieldType, Object lowerTerm, Object upperTerm, boolean includeLower,
            boolean includeUpper) {
        try {
            Method m = fieldType.getClass().getMethod("rangeQuery", Object.class, Object.class, boolean.class, boolean.class,
                    QueryShardContext.class);
            return (Query) m.invoke(fieldType, lowerTerm, upperTerm, includeLower, includeUpper, (QueryShardContext) null);
        } catch (NoSuchMethodException e) {
            return booleanFieldTypeRangeQueryOs(fieldType, lowerTerm, upperTerm, includeLower, includeUpper);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("BooleanFieldType.rangeQuery compatibility", e);
        }
    }

    private static Query booleanFieldTypeRangeQueryOs(MappedFieldType fieldType, Object lowerTerm, Object upperTerm, boolean includeLower,
            boolean includeUpper) {
        try {
            Class<?> qsc = Class.forName("org.opensearch.index.query.QueryShardContext");
            Method m = fieldType.getClass().getMethod("rangeQuery", Object.class, Object.class, boolean.class, boolean.class, qsc);
            return (Query) m.invoke(fieldType, lowerTerm, upperTerm, includeLower, includeUpper, null);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException("BooleanFieldType.rangeQuery compatibility", e);
        }
    }

    public static Engine indexShardEngine(IndexShard shard) {
        try {
            return (Engine) IndexShard.class.getMethod("getEngine").invoke(shard);
        } catch (NoSuchMethodException e) {
            try {
                Method m = IndexShard.class.getDeclaredMethod("getEngine");
                m.setAccessible(true);
                return (Engine) m.invoke(shard);
            } catch (ReflectiveOperationException e2) {
                throw new OpenSearchException("IndexShard.getEngine", e2);
            }
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new OpenSearchException(c);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException(e);
        }
    }

    public static IndexService indexShardIndexService(IndexShard shard) {
        try {
            return (IndexService) IndexShard.class.getMethod("indexService").invoke(shard);
        } catch (NoSuchMethodException e) {
            try {
                Method m = IndexShard.class.getDeclaredMethod("indexService");
                m.setAccessible(true);
                return (IndexService) m.invoke(shard);
            } catch (NoSuchMethodException e2) {
                try {
                    return (IndexService) IndexShard.class.getMethod("getIndexService").invoke(shard);
                } catch (ReflectiveOperationException e3) {
                    throw new OpenSearchException("IndexShard indexService accessor", e3);
                }
            } catch (ReflectiveOperationException e2) {
                throw new OpenSearchException("IndexShard indexService accessor", e2);
            }
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            throw new OpenSearchException(c);
        } catch (ReflectiveOperationException e) {
            throw new OpenSearchException(e);
        }
    }

    public static boolean indexVersionOnOrAfter600Beta1(IndexSettings settings) {
        try {
            Class<?> legacy = Class.forName("org.opensearch.LegacyESVersion");
            Object marker = legacy.getField("V_6_0_0_beta1").get(null);
            Object vCreated = settings.getIndexVersionCreated();
            return (Boolean) vCreated.getClass().getMethod("onOrAfter", legacy).invoke(vCreated, marker);
        } catch (Exception e) {
            try {
                Class<?> ver = Class.forName("org.opensearch.Version");
                Object marker = ver.getField("V_6_0_0_beta1").get(null);
                Object vCreated = settings.getIndexVersionCreated();
                return (Boolean) vCreated.getClass().getMethod("onOrAfter", ver).invoke(vCreated, marker);
            } catch (Exception e2) {
                return true;
            }
        }
    }

    public static boolean mappedFieldTypeIsSearchable(MappedFieldType fieldType) {
        try {
            Method m = fieldType.getClass().getMethod("isSearchable");
            return (Boolean) m.invoke(fieldType);
        } catch (Exception e) {
            try {
                Method im = fieldType.getClass().getMethod("indexOptions");
                Object opts = im.invoke(fieldType);
                Class<?> indexOptionsClass = Class.forName("org.apache.lucene.index.IndexOptions");
                Object none = indexOptionsClass.getField("NONE").get(null);
                return opts != null && !opts.equals(none);
            } catch (Exception e2) {
                return true;
            }
        }
    }

    /** Nested doc ordering: OpenSearch uses LegacyESVersion; ES 6.8 uses Version (both via reflection for one source tree). */
    public static boolean indexVersionOnOrAfter65(IndexSettings settings) {
        try {
            Class<?> legacy = Class.forName("org.opensearch.LegacyESVersion");
            Object v65 = legacy.getField("V_6_5_0").get(null);
            Object vCreated = settings.getIndexVersionCreated();
            return (Boolean) vCreated.getClass().getMethod("onOrAfter", legacy).invoke(vCreated, v65);
        } catch (Exception e) {
            try {
                Class<?> ver = Class.forName("org.opensearch.Version");
                Object v65 = ver.getField("V_6_5_0").get(null);
                Object vCreated = settings.getIndexVersionCreated();
                return (Boolean) vCreated.getClass().getMethod("onOrAfter", ver).invoke(vCreated, v65);
            } catch (Exception e2) {
                return false;
            }
        }
    }
}
