/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elassandra.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.ElassandraSchemaBridge;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.elassandra.index.mapper.internal.TokenFieldMapper;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.index.Index;
import org.opensearch.index.mapper.*;
import org.opensearch.index.mapper.CqlMapper.CqlCollection;
import org.opensearch.index.mapper.CqlMapper.CqlStruct;
import static org.opensearch.index.mapper.CqlMapper.CqlStruct.*;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Logger;

public class SchemaManager {
    final Logger logger = LogManager.getLogger(getClass());
    final ClusterService clusterService;
    final SchemaListener schemaListener;

    /**
     * Inhibited schema listeners avoid loops when applying new cluster state in CQL schema.
     */
    private final Collection<SchemaChangeListener> inhibitedSchemaListeners;

    public static final String GEO_POINT_TYPE = "geo_point";
    public static final ColumnIdentifier GEO_POINT_NAME = new ColumnIdentifier(GEO_POINT_TYPE, true);
    public static final String ATTACHMENT_TYPE = "attachement";
    public static final ColumnIdentifier ATTACHMENT_NAME = new ColumnIdentifier(ATTACHMENT_TYPE, true);
    public static final String COMPLETION_TYPE = "completion";
    public static final ColumnIdentifier COMPLETION_NAME = new ColumnIdentifier(COMPLETION_TYPE, true);

    static final Map<String, CQL3Type.Raw> GEO_POINT_FIELDS = ImmutableMap.of(
            org.opensearch.common.geo.GeoUtils.LATITUDE, CQL3Type.Raw.from(CQL3Type.Native.DOUBLE),
            org.opensearch.common.geo.GeoUtils.LONGITUDE, CQL3Type.Raw.from(CQL3Type.Native.DOUBLE));

    static final Map<String, CQL3Type.Raw> COMPLETION_FIELDS = new ImmutableMap.Builder<String, CQL3Type.Raw>()
            .put("input",  CQL3Type.Raw.list(CQL3Type.Raw.from(CQL3Type.Native.TEXT)))
            .put("contexts", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("weight", CQL3Type.Raw.from(CQL3Type.Native.INT))
            .build();

    static final Map<String, CQL3Type.Raw> ATTACHMENT_FIELDS = new ImmutableMap.Builder<String, CQL3Type.Raw>()
            .put("context", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("content_type", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("content_length", CQL3Type.Raw.from(CQL3Type.Native.BIGINT))
            .put("date", CQL3Type.Raw.from(CQL3Type.Native.TIMESTAMP))
            .put("title", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("author", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("keywords", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .put("language", CQL3Type.Raw.from(CQL3Type.Native.TEXT))
            .build();

    public static final String PERCOLATOR_TABLE = "_percolator";

    public static final String ELASTIC_ID_COLUMN_NAME = "_id";

    /** Same as forked {@code IndexMetadata#keyspace()} (OpenSearch {@code IndexMetadata} has no helper). */
    private static String keyspaceForIndex(IndexMetadata indexMetaData) {
        String ks = indexMetaData.getSettings().get("index.keyspace");
        if (ks != null) {
            return ks;
        }
        return ClusterService.indexToKsName(indexMetaData.getIndex().getName());
    }

    private static boolean hasVirtualIndex(IndexMetadata indexMetaData) {
        return indexMetaData.getSettings().get("index.virtual_index") != null;
    }

    private static boolean isOpaqueStorage(org.opensearch.index.IndexSettings indexSettings) {
        // Same keys as IndexMetadata.SETTING_INDEX_OPAQUE_STORAGE / ClusterService.SETTING_SYSTEM_INDEX_OPAQUE_STORAGE
        return indexSettings.getSettings().getAsBoolean("index.index_opaque_storage", Boolean.getBoolean("es.index_opaque_storage"));
    }

    private static String tableOptionsFromMapperService(MapperService mapperService) {
        return mapperService.getIndexSettings().getSettings().get("index.table_options");
    }

    /** ES 6 parent/child mapper; OpenSearch uses join fields — reflection keeps both builds compiling. */
    private static boolean needsParentColumn(DocumentMapper docMapper) {
        try {
            Object pfm = docMapper.getClass().getMethod("parentFieldMapper").invoke(docMapper);
            if (pfm == null) {
                return false;
            }
            if (!((Boolean) pfm.getClass().getMethod("active").invoke(pfm))) {
                return false;
            }
            return pfm.getClass().getMethod("pkColumns").invoke(pfm) == null;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    public static Map<String, String> cqlMapping = new ImmutableMap.Builder<String,String>()
            .put("text", "keyword")
            .put("varchar", "keyword")
            .put("timestamp", "date")
            .put("date", "date")
            .put("time", "long")
            .put("smallint", "short")
            .put("tinyint", "byte")
            .put("int", "integer")
            .put("bigint", "long")
            .put("double", "double")
            .put("float", "float")
            .put("boolean", "boolean")
            .put("blob", "binary")
            .put("inet", "ip" )
            .put("uuid", "keyword" )
            .put("timeuuid", "keyword" )
            .put("decimal", "keyword" )
            .build();

    public SchemaManager(Settings settings, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.schemaListener = new SchemaListener(settings, clusterService);
        this.inhibitedSchemaListeners = Collections.singletonList(this.schemaListener);
    }

    public SchemaListener getSchemaListener() {
        return schemaListener;
    }

    public Collection<SchemaChangeListener> getInhibitedSchemaListeners() {
        return inhibitedSchemaListeners;
    }

    public boolean isNativeCql3Type(String cqlType) {
        return cqlMapping.keySet().contains(cqlType) && !cqlType.startsWith("geo_");
    }

    // Because Cassandra table name does not support dash, convert dash to underscore in elasticsearch type, an keep this information
    // in a map for reverse lookup. Of course, conflict is still possible in a keyspace.
    private static final Map<String, String> cfNameToType = new ConcurrentHashMap<String, String>() {{
       put(PERCOLATOR_TABLE, "percolator");
    }};

    public static String typeToCfName(String keyspaceName, String typeName) {
        return typeToCfName(keyspaceName, typeName, false);
    }

    public static String typeToCfName(String keyspaceName, String typeName, boolean remove) {
        if (typeName.indexOf('-') >= 0) {
            String cfName = typeName.replaceAll("\\-", "_");
            if (remove) {
                cfNameToType.remove(keyspaceName+"."+cfName);
            } else {
                cfNameToType.put(keyspaceName+"."+cfName, typeName);
            }
            return cfName;
        }
        return typeName;
    }

    public String typeToCfName(TableMetadata cfm, String typeName, boolean remove) {
        return SchemaManager.typeToCfName(cfm.keyspace, typeName, remove);
    }

    public static String cfNameToType(String keyspaceName, String cfName) {
        if (cfName.indexOf('_') >= 0) {
            String type = cfNameToType.get(keyspaceName+"."+cfName);
            if (type != null)
                return type;
        }
        return cfName;
    }

    public static String buildIndexName(final String cfName) {
        return new StringBuilder("elastic_")
            .append(cfName)
            .append("_idx").toString();
    }

    public static TableMetadata getTableMetadata(final String ksName, final String cfName) throws ActionRequestValidationException {
        TableMetadata metadata = Schema.instance.getTableMetadata(ksName, cfName);
        if (metadata == null) {
            ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationError(ksName+"."+cfName+" table does not exists");
            throw arve;
        }
        return metadata;
    }

    public static KeyspaceMetadata getKSMetaData(final String ksName) throws ActionRequestValidationException {
        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(ksName);
        if (metadata == null) {
            ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationError("Keyspace " + ksName + " does not exists");
            throw arve;
        }
        return metadata;
    }

    public static KeyspaceMetadata getKSMetaDataCopy(final String ksName) {
        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(ksName);
        return metadata;
    }

    private Pair<KeyspaceMetadata, UserType> createUserTypeIfNotExists(KeyspaceMetadata ksm, String typeName, Map<String, CQL3Type.Raw> fields,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) throws RequestExecutionException
    {
        ColumnIdentifier ci = new ColumnIdentifier(typeName, true);
        Optional<UserType> typeOption = getType(ksm, ci);
        if (typeOption.isPresent())
            return Pair.create(ksm, typeOption.get());

        logger.debug("create type keyspace=[{}] name=[{}] fields={}", ksm.name, typeName, fields);
        List<FieldIdentifier> fieldNames = new ArrayList<>();
        List<CQL3Type.Raw> rawFieldTypes = new ArrayList<>();
        for (Map.Entry<String, CQL3Type.Raw> field : fields.entrySet()) {
            fieldNames.add(FieldIdentifier.forInternalString(field.getKey()));
            rawFieldTypes.add(field.getValue());
        }
        Set<FieldIdentifier> usedNames = new HashSet<>();
        for (FieldIdentifier name : fieldNames) {
            if (!usedNames.add(name))
                throw new InvalidRequestException(String.format(Locale.ROOT, "Duplicate field name '%s' in type '%s'", name, typeName));
        }
        for (CQL3Type.Raw type : rawFieldTypes) {
            if (type.isCounter())
                throw new InvalidRequestException("A user type cannot contain counters");
            if (type.isUDT() && !type.isFrozen())
                throw new InvalidRequestException("A user type cannot contain non-frozen UDTs");
        }
        List<AbstractType<?>> fieldTypes =
                rawFieldTypes.stream()
                        .map(t -> t.prepare(ksm.name, ksm.types).getType())
                        .collect(Collectors.toList());
        UserType userType = new UserType(ksm.name, ByteBufferUtil.bytes(typeName), fieldNames, fieldTypes, true);

        Mutation.SimpleBuilder builder = ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros());
        SchemaKeyspace.addTypeToSchemaMutation(userType, builder);
        mutations.add(builder.build());

        events.add(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksm.name, typeName));
        KeyspaceMetadata ksm2 = ksm.withSwapped(ksm.types.with(userType));
        return Pair.create(ksm2, userType);
    }

    private Pair<KeyspaceMetadata, UserType> createOrUpdateUserType(KeyspaceMetadata ksm, String typeName, Map<String, CQL3Type.Raw> fields,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        ColumnIdentifier ci = new ColumnIdentifier(typeName, true);
        Optional<UserType> userTypeOption = getType(ksm, ci);
        if (!userTypeOption.isPresent()) {
            return createUserTypeIfNotExists(ksm, typeName, fields, mutations, events);
        } else {
            KeyspaceMetadata ksmOut = ksm;
            UserType userType = userTypeOption.get();
            logger.trace("update keyspace.type=[{}].[{}] fields={}", ksm.name, typeName, fields);
            for (Map.Entry<String, CQL3Type.Raw> field : fields.entrySet()) {
                FieldIdentifier fieldIdentifier = FieldIdentifier.forInternalString(field.getKey());
                int i = userType.fieldPosition(fieldIdentifier);
                if (i == -1) {
                    logger.trace("add field to keyspace.type=[{}].[{}] field={}", ksm.name, typeName, fieldIdentifier);
                    CQL3Type.Raw rawType = field.getValue();
                    if (rawType.isCounter())
                        throw new InvalidRequestException("A user type cannot contain counters");
                    if (rawType.isUDT() && !rawType.isFrozen())
                        throw new InvalidRequestException("A user type cannot contain non-frozen UDTs");
                    AbstractType<?> fieldType = rawType.prepare(ksmOut.name, ksmOut.types).getType();
                    if (fieldType.referencesUserType(userType.name))
                        throw new InvalidRequestException("Cannot add field that would create a circular reference");
                    List<FieldIdentifier> fn = new ArrayList<>(userType.fieldNames());
                    fn.add(fieldIdentifier);
                    List<AbstractType<?>> ft = new ArrayList<>(userType.fieldTypes());
                    ft.add(fieldType);
                    userType = new UserType(ksmOut.name, userType.name, fn, ft, true);

                    Mutation.SimpleBuilder builder = ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksmOut.name, ksmOut.params, FBUtilities.timestampMicros());
                    SchemaKeyspace.addTypeToSchemaMutation(userType, builder);
                    mutations.add(builder.build());
                    events.add(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksmOut.name, typeName));
                    ksmOut = ksmOut.withUpdatedUserType(userType);
                } else {
                    CQL3Type newType = field.getValue().prepare(ksmOut.name, ksmOut.types);
                    CQL3Type existingType = userType.fieldType(i).asCQL3Type();
                    if (!newType.getType().isCompatibleWith(existingType.getType())) {
                        throw new InvalidRequestException(
                                String.format(Locale.ROOT, "Field \"%s\" with type %s does not match updated type %s",
                                        field.getKey(), existingType, newType));
                    }
                }
            }
            return Pair.create(ksmOut, userType);
        }
    }

    private Optional<UserType> getType(KeyspaceMetadata ksm, ColumnIdentifier typeName) {
        return Optional.ofNullable(ksm.types.getNullable(ByteBufferUtil.bytes(typeName.toString())));
    }

    private Pair<KeyspaceMetadata, CQL3Type.Raw> createRawTypeIfNotExists(KeyspaceMetadata ksm,
            String typeName,
            Map<String, CQL3Type.Raw> fields,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        Pair<KeyspaceMetadata, UserType> x = createUserTypeIfNotExists(ksm, typeName, fields, mutations, events);
        UTName ut = new UTName(new  ColumnIdentifier(ksm.name, true), new  ColumnIdentifier(x.right.getNameAsString(), true));
        CQL3Type.Raw type = CQL3Type.Raw.userType(ut);
        type.freeze();
        return Pair.create(x.left, type);
    }

    public KeyspaceMetadata createOrUpdateKeyspace(final String ksName,
            final int replicationFactor,
            final Map<String, Integer> replicationMap,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        KeyspaceMetadata ksm;
        Keyspace ks = null;
        try {
            ks = Keyspace.open(ksName);
            if (ks != null && !(ks.getReplicationStrategy() instanceof NetworkTopologyStrategy)) {
                throw new SettingsException("Cannot create index, underlying keyspace requires the NetworkTopologyStrategy.");
            }
        } catch(AssertionError | NullPointerException e) {
        }
        if (ks != null) {
            // TODO: check replication
            ksm = ks.getMetadata();
        } else {
            Map<String, String> replication = new HashMap<>();
            replication.put("class", "NetworkTopologyStrategy");
            replication.put(DatabaseDescriptor.getLocalDataCenter(), Integer.toString(replicationFactor));
            for(Map.Entry<String, Integer> entry : replicationMap.entrySet())
                replication.put(entry.getKey(), Integer.toString(entry.getValue()));
            logger.trace("Creating new keyspace [{}] with replication={}", ksName, replication);

            KeyspaceParams params = KeyspaceParams.create(true, replication);
            ksm = KeyspaceMetadata.create(ksName, params);
            mutations.add(ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksm, FBUtilities.timestampMicros()).build());
            events.add(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
        }
        return ksm;
    }

    private KeyspaceMetadata createTable(final KeyspaceMetadata ksm, String cfName,
            Map<String, ColumnDescriptor> columnsMap,
            String tableOptions,
            final Collection<IndexMetadata> siblings, // include the indexMetaData and all other indices having a mapping to the same table.
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) throws IOException, RecognitionException {
        QualifiedName qn = new QualifiedName(ksm.name, cfName);
        CreateTableStatement.Raw cts = new CreateTableStatement.Raw(qn, true);

        logger.debug("columnsMap="+columnsMap);
        List<ColumnDescriptor> columnsList = new ArrayList<>();
        columnsList.addAll(columnsMap.values());
        Collections.sort(columnsList); // sort primary key columns

        List<ColumnIdentifier> pk = new ArrayList<>();
        for (ColumnDescriptor cd : columnsList) {
            ColumnIdentifier ci = ColumnIdentifier.getInterned(cd.name, true);
            cts.addColumn(ci, cd.type, cd.kind == ColumnMetadata.Kind.STATIC);
            if (cd.kind == ColumnMetadata.Kind.PARTITION_KEY)
                pk.add(ci);
            if (cd.kind == ColumnMetadata.Kind.CLUSTERING) {
                cts.markClusteringColumn(ci);
                cts.extendClusteringOrder(ci, !cd.desc);
            }
        }
        cts.setPartitionKeyColumns(pk);

        if (tableOptions != null && tableOptions.length() > 0) {
            CQLFragmentParser.parseAnyUnhandled(new CQLFragmentParser.CQLParserFunction<TableAttributes>() {
                @Override
                public TableAttributes parse(CqlParser parser) throws RecognitionException {
                    parser.properties(cts.attrs);
                    return cts.attrs;
                }
            }, tableOptions);
        }

        CreateTableStatement prepared = cts.prepare(ClientState.forInternalCalls());
        TableMetadata cfm = prepared.builder(ksm.types).build();
        cfm = updateTableExtensions(ksm, cfm, siblings);

        Mutation.SimpleBuilder builder = ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros());
        ElassandraSchemaBridge.addTableToSchemaMutation(cfm, true, builder);
        mutations.add(builder.build());

        events.add(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksm.name, cfName));
        return ksm.withSwapped(ksm.tables.with(cfm));
    }

    // add only new columns and update table extension with mapping metadata
    private KeyspaceMetadata updateTable(final KeyspaceMetadata ksm, String cfName,
            Map<String, ColumnDescriptor> columnsMap,
            TableAttributes tableAttrs,
            final Collection<IndexMetadata> siblings,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        TableMetadata cfm = ksm.getTableOrViewNullable(cfName);
        if (cfm == null)
            throw new InvalidRequestException(String.format(Locale.ROOT, "Table '%s.%s' doesn't exist", ksm.name, cfName));

        TableMetadata.Builder builder = cfm.unbuild();
        List<ColumnDescriptor> newCols = columnsMap.values().stream()
                .filter(cd -> !cd.exists())
                .collect(Collectors.toList());
        if (newCols.size() > 0) {
            logger.debug("table {}.{} add columnsMap={}", ksm.name, cfName, columnsMap);
            for (ColumnDescriptor cd : newCols)
                builder.addColumn(cd.createColumnMetadata(ksm, cfName));
        }
        if (tableAttrs != null) {
            builder.params(tableAttrs.asAlteredTableParams(cfm.params));
        }
        TableMetadata x = builder.build();
        x = updateTableExtensions(ksm, x, siblings);
        mutations.add(ElassandraSchemaBridge.makeUpdateTableMutation(ksm, cfm, x, FBUtilities.timestampMicros()).build());

        KeyspaceMetadata ksm2 = ksm.withSwapped(ksm.tables.without(cfm.name).with(x));
        events.add(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksm.name, cfName));
        return ksm2;
    }

    public TableMetadata removeTableExtensionToMutationBuilder(TableMetadata cfm, final Set<IndexMetadata> indexMetaDataSet, Mutation.SimpleBuilder builder) {
        Map<String, ByteBuffer> extensions = new LinkedHashMap<>();
        if (cfm.params != null && cfm.params.extensions != null) {
            Set<String> toRemoveExtentsions = indexMetaDataSet.stream().map(imd -> clusterService.getExtensionKey(imd)).collect(Collectors.toSet());
            extensions = cfm.params.extensions.entrySet().stream()
                .filter( x -> !toRemoveExtentsions.contains(x.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
        }
        TableMetadata cfm2 = cfm.unbuild().params(cfm.params.unbuild().extensions(extensions).build()).build();
        SchemaKeyspace.addTableExtensionsToSchemaMutation(cfm, extensions, builder);
        return cfm2;
    }

    private Pair<KeyspaceMetadata, CQL3Type.Raw> createOrUpdateRawType(final KeyspaceMetadata ksm, final String cfName, final String name, final CqlMapper mapper,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        KeyspaceMetadata ksm2 = ksm;
        CQL3Type.Raw type;
        if (mapper instanceof ObjectMapper) {
            ColumnIdentifier ksCi = new  ColumnIdentifier(ksm.name, true);
            ObjectMapper objectMapper = (ObjectMapper) mapper;
            Map<String, CQL3Type.Raw> fields = new HashMap<>();
            for (Iterator<Mapper> it = objectMapper.iterator(); it.hasNext(); ) {
                Mapper m = it.next();
                if (m instanceof ObjectMapper && ((ObjectMapper) m).isEnabled() && !m.hasField()) {
                    continue;   // ignore object with no sub-field #146
                }

                // Use only the last part of the fullname to build UDT.
                int lastDotIndex = m.name().lastIndexOf('.');
                String fieldName = (lastDotIndex > 0) ? m.name().substring(lastDotIndex+1) :  m.name();
                Pair<KeyspaceMetadata, CQL3Type.Raw> x = createOrUpdateRawType(ksm2, cfName, fieldName, (CqlMapper) m, mutations, events);
                ksm2 = x.left;
                fields.put(fieldName, x.right);
            }
            String typeName = (objectMapper.cqlUdtName() == null) ? cfName + "_" + objectMapper.fullPath().replaceAll("(\\.|\\-)", "_") : objectMapper.cqlUdtName();
            Pair<KeyspaceMetadata, UserType> x = createOrUpdateUserType(ksm2, typeName, fields, mutations, events);
            ksm2 = x.left;
            UTName ut = new UTName(ksCi, new  ColumnIdentifier(typeName, true));
            type = CQL3Type.Raw.userType(ut);
            type.freeze();
        } else if (mapper instanceof GeoPointFieldMapper) {
            Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm2, GEO_POINT_TYPE, GEO_POINT_FIELDS, mutations, events);
            ksm2 = x.left;
            type = x.right;
        } else if (mapper instanceof RangeFieldMapper) {
            Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm2,
                    ((RangeFieldMapper) mapper).fieldType().typeName(),
                    ((RangeFieldMapper) mapper).cqlFieldTypes(),
                    mutations, events);
            ksm2 = x.left;
            type = x.right;
        } else if (mapper instanceof FieldMapper) {
            type = ((FieldMapper)mapper).rawType();
        } else {
            throw new ConfigurationException("Unkown mapper class="+mapper.getClass().getName());
        }
        return Pair.create(ksm2, mapper.collection(type));
    }

    private Pair<KeyspaceMetadata, CQL3Type.Raw> buildObject(final KeyspaceMetadata ksm, final String cfName, final String name, final ObjectMapper objectMapper,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) throws RequestExecutionException {
        switch(objectMapper.cqlStruct()) {
        case UDT:
            if (!objectMapper.iterator().hasNext())
                throw new ConfigurationException("Cannot build UDT for empty object ["+name+"]");
            return createOrUpdateRawType(ksm, cfName, name, objectMapper, mutations, events);
        case MAP:
        case OPAQUE_MAP:
            if (objectMapper.iterator().hasNext()) {
                Mapper childMapper = objectMapper.iterator().next();
                if (childMapper instanceof FieldMapper) {
                    //return "map<text,"+childMapper.cqlType()+">";
                    return Pair.create(ksm, CQL3Type.Raw.map(CQL3Type.Raw.from(CQL3Type.Native.TEXT), ((FieldMapper)childMapper).rawType()));
                } else if (childMapper instanceof ObjectMapper) {
                    //String subType = buildCql(ksName,cfName,childMapper.simpleName(),(ObjectMapper)childMapper, updatedUserTypes, validateOnly);
                    //return (subType==null) ? null : "map<text,frozen<"+subType+">>";
                    Pair<KeyspaceMetadata, CQL3Type.Raw> x = buildObject(ksm, cfName, childMapper.simpleName(), (ObjectMapper)childMapper, mutations, events);
                    return (x.right == null) ? null : Pair.create(x.left, CQL3Type.Raw.map(CQL3Type.Raw.from(CQL3Type.Native.TEXT), x.right).freeze());
                }
            }
            // default map prototype, no mapper to determine the value type.
            return Pair.create(ksm, CQL3Type.Raw.map(CQL3Type.Raw.from(CQL3Type.Native.TEXT), CQL3Type.Raw.from(CQL3Type.Native.TEXT)));
        default:
            throw new ConfigurationException("Object ["+name+"] not supported");
        }
    }

    public void dropIndexKeyspace(final String ksName,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) throws ConfigurationException {
        if (SchemaConstants.isSystemKeyspace(ksName))
            throw new ConfigurationException("Cannot drop a system keyspace.");
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        mutations.add(ElassandraSchemaBridge.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()).build());
        events.add(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
    }

    /**
     * Update table extensions when index settings change. This allow to get index settings+mappings in the CQL backup of the table.
     */
    public TableMetadata updateTableExtensions(final KeyspaceMetadata ksm, final TableMetadata cfm, final Collection<IndexMetadata> siblings) {
        Map<String, ByteBuffer> extensions = new LinkedHashMap<>();
        if (cfm.params != null && cfm.params.extensions != null)
            extensions.putAll(cfm.params.extensions);

        for(IndexMetadata imd : siblings) {
            assert ksm.name.equals(keyspaceForIndex(imd)) : "Keyspace metadata="+ksm.name+" does not match indexMetadata.keyspace="+keyspaceForIndex(imd);
            clusterService.putIndexMetaDataExtension(imd, extensions);
        }

        return cfm.unbuild().params(cfm.params.unbuild().extensions(extensions).build()).build();
    }

    /**
     * Populate the columnsMap of a table for the provided indeMetaData/mapperService
     */
    private KeyspaceMetadata buildColumns(final KeyspaceMetadata ksm2,
            final TableMetadata cfm,
            final String type,
            final IndexMetadata indexMetaData,
            final MapperService mapperService,
            Map<String, ColumnDescriptor> columnsMap,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {

        KeyspaceMetadata ksm = ksm2;
        String cfName = typeToCfName(ksm.name, type);
        boolean newTable = (cfm == null);

        DocumentMapper docMapper = mapperService.documentMapper(type);
        MappingMetadata mappingMd = indexMetaData.getMappings().get(type);
        Map<String, Object> mappingMap = mappingMd.sourceAsMap();

        Set<String> columns = new HashSet();
        if (isOpaqueStorage(mapperService.getIndexSettings())) {
            columns.add(SourceFieldMapper.NAME);
        } else {
            if (docMapper.sourceMapper().enabled())
                columns.add(SourceFieldMapper.NAME);
            if (mappingMap.get("properties") != null)
                columns.addAll(((Map<String, Object>) mappingMap.get("properties")).keySet());
        }

        logger.debug("Updating CQL3 schema {}.{} columns={}", ksm.name, cfName, columns);
        for (String column : columns) {
            if (isReservedKeyword(column))
                logger.warn("Allowing a CQL reserved keyword in ES: {}", column);

            if (column.equals(TokenFieldMapper.NAME))
                continue; // ignore pseudo column known by Elasticsearch

            ColumnDescriptor colDesc = new ColumnDescriptor(column);
            FieldMapper fieldMapper = SourceFieldMapper.NAME.equals(column)
                    ? docMapper.sourceMapper()
                    : docMapper.mappers().smartNameFieldMapper(column);
            ColumnMetadata cdef = (newTable) ? null : cfm.getColumn(new ColumnIdentifier(column, true));

            if (fieldMapper != null) {
                if (fieldMapper.cqlCollection().equals(CqlCollection.NONE) && (fieldMapper instanceof SourceFieldMapper) == false)
                    continue; // ignore field.

                if (fieldMapper instanceof RangeFieldMapper) {
                    RangeFieldMapper rangeFieldMapper = (RangeFieldMapper) fieldMapper;
                    if (cdef != null) {
                        // index range stored as a range UDT in cassandra.
                        if (!(cdef.type instanceof UserType))
                            throw new MapperParsingException("Column ["+column+"] is not a Cassandra User Defined Type to store an Elasticsearch range");
                        colDesc.type = CQL3Type.Raw.from(CQL3Type.UserDefined.create((UserType)cdef.type));
                    } else {
                        // create a range UDT to store range fields
                        Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm,
                                rangeFieldMapper.fieldType().typeName(),
                                rangeFieldMapper.cqlFieldTypes(),
                                mutations, events);
                        ksm = x.left;
                        colDesc.type = x.right;
                    }
                } else if (fieldMapper instanceof GeoPointFieldMapper) {
                    if (cdef != null && cdef.type instanceof UTF8Type) {
                        // index geohash stored as text in cassandra.
                        colDesc.type = CQL3Type.Raw.from(CQL3Type.Native.TEXT);
                    } else {
                        // create a geo_point UDT to store lat,lon
                        Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm, GEO_POINT_TYPE, GEO_POINT_FIELDS, mutations, events);
                        ksm = x.left;
                        colDesc.type = x.right;
                    }
                } else if (fieldMapper instanceof GeoShapeFieldMapper) {
                    colDesc.type = CQL3Type.Raw.from(CQL3Type.Native.TEXT);
                } else if (fieldMapper instanceof CompletionFieldMapper) {
                    Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm, COMPLETION_TYPE, COMPLETION_FIELDS, mutations, events);
                    ksm = x.left;
                    colDesc.type = x.right;
                } else if (fieldMapper.getClass().getName().equals("org.opensearch.mapper.attachments.AttachmentMapper")) {
                    // attachement is a plugin, so class may not found.
                    Pair<KeyspaceMetadata, CQL3Type.Raw> x = createRawTypeIfNotExists(ksm, ATTACHMENT_TYPE, ATTACHMENT_FIELDS, mutations, events);
                    ksm = x.left;
                    colDesc.type = x.right;
                } else if (fieldMapper instanceof SourceFieldMapper) {
                    colDesc.type = CQL3Type.Raw.from(CQL3Type.Native.BLOB);
                } else {
                    colDesc.type = fieldMapper.rawType();
                    if (colDesc.type == null) {
                        logger.warn("Ignoring field [{}] type [{}]", column, fieldMapper.name());
                        continue;
                    }
                }

                if (fieldMapper.cqlPrimaryKeyOrder() >= 0) {
                    colDesc.position = fieldMapper.cqlPrimaryKeyOrder();
                    if (fieldMapper.cqlPartitionKey()) {
                        colDesc.kind = ColumnMetadata.Kind.PARTITION_KEY;
                    } else {
                        colDesc.kind = ColumnMetadata.Kind.CLUSTERING;
                        colDesc.desc = fieldMapper.cqlClusteringKeyDesc();
                    }
                }
                if (fieldMapper.cqlStaticColumn())
                    colDesc.kind = ColumnMetadata.Kind.STATIC;
                colDesc.type = fieldMapper.collection(colDesc.type);
            } else {
                ObjectMapper objectMapper = docMapper.objectMappers().get(column);
                if (objectMapper == null) {
                   logger.warn("Cannot infer CQL type from object mapping for field [{}], ignoring", column);
                   continue;
                }
                if (objectMapper.cqlCollection().equals(CqlCollection.NONE))
                    continue; // ignore field

                if (!objectMapper.isEnabled()) {
                    logger.debug("Object [{}] not enabled stored as text", column);
                    colDesc.type = CQL3Type.Raw.from(CQL3Type.Native.TEXT);
                } else if (objectMapper.cqlStruct().equals(CqlStruct.MAP) || (objectMapper.cqlStruct().equals(CqlStruct.OPAQUE_MAP))) {
                    // TODO: check columnName exists and is map<text,?>
                    Pair<KeyspaceMetadata, CQL3Type.Raw> x = buildObject(ksm, cfName, column, objectMapper, mutations, events);
                    colDesc.type = x.right;
                    ksm = x.left;
                    //if (!objectMapper.cqlCollection().equals(CqlCollection.SINGLETON)) {
                    //    colDesc.type = objectMapper.cqlCollectionTag()+"<"+colDesc.type+">";
                    //}
                    //logger.debug("Expecting column [{}] to be a map<text,?>", column);
                } else  if (objectMapper.cqlStruct().equals(CqlStruct.UDT)) {
                    if (!objectMapper.hasField()) {
                        logger.debug("Ignoring [{}] has no sub-fields", column); // no sub-field, ignore it #146
                        continue;
                    }
                    Pair<KeyspaceMetadata, CQL3Type.Raw> x = buildObject(ksm, cfName, column, objectMapper, mutations, events);
                    ksm = x.left;
                    colDesc.type = x.right;
                    /*
                    if (!objectMapper.cqlCollection().equals(CqlCollection.SINGLETON) && !(cfName.equals(PERCOLATOR_TABLE) && column.equals("query"))) {
                        colDesc.type = objectMapper.collection(colDesc.type);
                    }
                    */
                }
                if (objectMapper.cqlPrimaryKeyOrder() >= 0) {
                    colDesc.position = objectMapper.cqlPrimaryKeyOrder();
                    if (objectMapper.cqlPartitionKey()) {
                        colDesc.kind = ColumnMetadata.Kind.PARTITION_KEY;
                    } else {
                        colDesc.kind = ColumnMetadata.Kind.CLUSTERING;
                        colDesc.desc = objectMapper.cqlClusteringKeyDesc();
                    }
                }
                if (objectMapper.cqlStaticColumn())
                    colDesc.kind = ColumnMetadata.Kind.STATIC;
            }
            columnsMap.putIfAbsent(colDesc.name, colDesc);
        }

        // add _parent column if necessary. Parent and child documents should have the same partition key.
        if (needsParentColumn(docMapper))
            columnsMap.putIfAbsent("_parent", new ColumnDescriptor("_parent", CQL3Type.Raw.from(CQL3Type.Native.TEXT)));

        logger.debug("columnsMap={}", columnsMap);
        return ksm;
    }

    /**
     * Create table for all IndexMetadata having a mapping.
     * WARNING: schema mutations are applied in a random order and table extensions is replace by the last one.
     */
    public KeyspaceMetadata updateTableSchema(final KeyspaceMetadata ksm2,
            final String type,
            final Map<Index, Pair<IndexMetadata, MapperService>> indiceMap,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        String query = null;
        String ksName = null;
        String cfName = null;
        Map<String, Object> mappingMap = null;

        try {
            KeyspaceMetadata ksm = ksm2;
            ksName = ksm2.name;
            cfName = typeToCfName(ksName, type);

            final TableMetadata cfm = ksm.getTableOrViewNullable(cfName);
            boolean newTable = (cfm == null);

            MapperService mapperService = null; // set with one of the IndexMetadata !

            Map<String, ColumnDescriptor> columnsMap = new HashMap<>();
            for(Pair<IndexMetadata, MapperService> pair : indiceMap.values()) {
                IndexMetadata indexMetaData = pair.left;
                if (hasVirtualIndex(indexMetaData))
                    continue;
                mapperService = pair.right;
                ksm = buildColumns(ksm, cfm, type, indexMetaData, mapperService, columnsMap, mutations, events);
            }

            if (newTable) {
                boolean hasPartitionKey = false;
                for(ColumnDescriptor cd : columnsMap.values()) {
                    if (cd.kind == ColumnMetadata.Kind.PARTITION_KEY) {
                        hasPartitionKey = true;
                        break;
                    }
                }
                if (!hasPartitionKey)
                    columnsMap.putIfAbsent(ELASTIC_ID_COLUMN_NAME, new ColumnDescriptor(ELASTIC_ID_COLUMN_NAME, CQL3Type.Raw.from(CQL3Type.Native.TEXT), ColumnMetadata.Kind.PARTITION_KEY, 0));
                ksm = createTable(ksm, cfName, columnsMap, tableOptionsFromMapperService(mapperService), indiceMap.values().stream().map(p->p.left).collect(Collectors.toList()), mutations, events);
            } else {
                // check column properties matches existing ones, or add it to columnsDefinitions
                for(ColumnDescriptor cd : columnsMap.values())
                    cd.validate(ksm, cfm);
                TableAttributes tableAttrs = new TableAttributes();
                ksm = updateTable(ksm, cfName, columnsMap, tableAttrs, indiceMap.values().stream().map(p->p.left).collect(Collectors.toList()), mutations, events);
            }


            String secondaryIndexClazz = mapperService.getIndexSettings().getSettings().get(ClusterService.SETTING_CLUSTER_SECONDARY_INDEX_CLASS,
                    clusterService.state().metadata().settings().get(ClusterService.SETTING_CLUSTER_SECONDARY_INDEX_CLASS,
                            ClusterService.defaultSecondaryIndexClass.getName()));
            ksm = createSecondaryIndexIfNotExists(ksm, cfName, secondaryIndexClazz, mutations, events);
            return ksm;
        } catch (AssertionError | RequestValidationException e) {
            logger.error("Failed to execute table="+ksName+"."+cfName+" query=" + query + " mapping="+mappingMap , e);
            throw new MapperParsingException("Failed to execute query:" + query + " : "+e.getMessage(), e);
        } catch (Throwable e) {
            throw new MapperParsingException(e.getMessage(), e);
        }
    }

    // see https://docs.datastax.com/en/cql/3.0/cql/cql_reference/keywords_r.html
    public static final Pattern keywordsPattern = Pattern.compile("(ADD|ALLOW|ALTER|AND|ANY|APPLY|ASC|AUTHORIZE|BATCH|BEGIN|BY|COLUMNFAMILY|CREATE|DELETE|DESC|DROP|EACH_QUORUM|GRANT|IN|INDEX|INET|INSERT|INTO|KEYSPACE|KEYSPACES|LIMIT|LOCAL_ONE|LOCAL_QUORUM|MODIFY|NOT|NORECURSIVE|OF|ON|ONE|ORDER|PASSWORD|PRIMARY|QUORUM|RENAME|REVOKE|SCHEMA|SELECT|SET|TABLE|TO|TOKEN|THREE|TRUNCATE|TWO|UNLOGGED|UPDATE|USE|USING|WHERE|WITH)");

    public static boolean isReservedKeyword(String identifier) {
        return keywordsPattern.matcher(identifier.toUpperCase(Locale.ROOT)).matches();
    }

    private KeyspaceMetadata createSecondaryIndexIfNotExists(final KeyspaceMetadata ksm,
            final String tableName,
            final String className,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) {
        KeyspaceMetadata ksm2 = ksm;
        TableMetadata cfm0 = ksm.getTableOrViewNullable(tableName);
        assert cfm0 != null : "Table "+tableName+" not found in keyspace metadata";

        String idxName = buildIndexName(tableName);
        if (cfm0.indexes.get(idxName).isPresent())
            return ksm2;

        logger.debug("Create secondary index on table {}.{}", ksm.name, tableName);
        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, className);
        org.apache.cassandra.schema.IndexMetadata cassandraIndex =
            org.apache.cassandra.schema.IndexMetadata.fromIndexTargets(
                Collections.emptyList(), idxName, org.apache.cassandra.schema.IndexMetadata.Kind.CUSTOM, options);
        cassandraIndex.validate(cfm0);
        TableMetadata cfm = cfm0.withSwapped(cfm0.indexes.with(cassandraIndex));
        ksm2 = ksm.withSwapped(ksm.tables.without(cfm0.name).with(cfm));

        Mutation.SimpleBuilder builder = ElassandraSchemaBridge.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros());
        SchemaKeyspace.addUpdatedIndexToSchemaMutation(cfm, cassandraIndex, builder);
        mutations.add(builder.build());

        events.add(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksm.name, tableName));
        return ksm2;
    }

    public KeyspaceMetadata dropSecondaryIndex(final KeyspaceMetadata ksm,
            final TableMetadata cfm0,
            final Collection<Mutation> mutations,
            final Collection<Event.SchemaChange> events) throws RequestExecutionException  {
        KeyspaceMetadata ksm2 = ksm;
        for (org.apache.cassandra.schema.IndexMetadata idx : cfm0.indexes) {
            if (idx.isCustom() && idx.name.startsWith("elastic_")) {
                String className = idx.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
                if (className != null && className.endsWith("ElasticSecondaryIndex")) {
                    TableMetadata withoutIdx = cfm0.withSwapped(cfm0.indexes.without(idx.name));
                    TableMetadata cfm = withoutIdx.unbuild().params(withoutIdx.params.unbuild().extensions(Collections.emptyMap()).build()).build();
                    ksm2 = ksm.withSwapped(ksm.tables.without(cfm0.name).with(cfm));

                    logger.info("Drop secondary index '{}'", idx.name);
                    mutations.add(ElassandraSchemaBridge.makeUpdateTableMutation(ksm, cfm0, cfm, FBUtilities.timestampMicros()).build());
                    events.add(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksm.name, cfm.name));
                    break;
                }
            }
        }
        return ksm2;
    }

    public void dropSecondaryIndices(final IndexMetadata indexMetaData, final Collection<Mutation> mutations, final Collection<Event.SchemaChange> events)
            throws RequestExecutionException {
        String ksName = keyspaceForIndex(indexMetaData);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);
        for (TableMetadata cfm : ksm.tablesAndViews()) {
            if (org.apache.cassandra.schema.TableMetadata.Flag.isCQLTable(cfm.flags))
                dropSecondaryIndex(ksm, cfm, mutations, events);
        }
    }

    public void dropSecondaryIndex(KeyspaceMetadata ksm, String cfName, final Collection<Mutation> mutations, final Collection<Event.SchemaChange> events)
            throws RequestExecutionException {
        TableMetadata cfm = Schema.instance.getTableMetadata(ksm.name, cfName);
        if (cfm != null)
            dropSecondaryIndex(ksm, cfm, mutations, events);
    }

    public KeyspaceMetadata dropTables(KeyspaceMetadata ksm, final IndexMetadata indexMetaData, final Collection<Mutation> mutations, final Collection<Event.SchemaChange> events)
            throws RequestExecutionException {
        String ksName = keyspaceForIndex(indexMetaData);
        for(ObjectCursor<String> cursor : indexMetaData.getMappings().keys()) {
            TableMetadata cfm = Schema.instance.getTableMetadata(ksName, cursor.value);
            ksm = dropTable(ksm, cfm, mutations, events);
        }
        return ksm;
    }

    public KeyspaceMetadata dropTable(final KeyspaceMetadata ksm, final String table, final Collection<Mutation> mutations, final Collection<Event.SchemaChange> events)
            throws RequestExecutionException  {
        TableMetadata cfm = Schema.instance.getTableMetadata(ksm.name, table);
        return (cfm != null) ? dropTable(ksm, cfm, mutations, events) : ksm;
    }

    public KeyspaceMetadata dropTable(final KeyspaceMetadata ksm, final TableMetadata cfm, final Collection<Mutation> mutations, final Collection<Event.SchemaChange> events)
            throws RequestExecutionException  {
        KeyspaceMetadata ksm2 = ksm;
        if (cfm != null) {
            ksm2 = ksm.withSwapped(ksm.tables.without(cfm.name));

            logger.info("Drop table '{}'", cfm.name);
            mutations.add(ElassandraSchemaBridge.makeDropTableMutation(ksm, cfm, FBUtilities.timestampMicros()).build());
            events.add(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksm.name, cfm.name));
        }
        return ksm2;
    }
}
