package org.opensearch.index.mapper;

import org.apache.cassandra.cql3.CQL3Type;

import java.nio.ByteBuffer;

/**
 * CQL mapping metadata carried by forked mappers. Default methods allow stock {@code ObjectMapper} /
 * {@code FieldMapper} (OpenSearch side-car) to implement this interface for compilation; the Elassandra
 * fork overrides with real behaviour.
 */
public interface CqlMapper {

    enum CqlCollection {
        LIST, SET, SINGLETON, NONE
    }

    enum CqlStruct {
        UDT, MAP, OPAQUE_MAP, TUPLE
    }

    default CqlCollection cqlCollection() {
        return CqlCollection.NONE;
    }

    default String cqlCollectionTag() {
        return "";
    }

    default CqlStruct cqlStruct() {
        return CqlStruct.UDT;
    }

    default boolean cqlPartialUpdate() {
        return false;
    }

    default boolean cqlPartitionKey() {
        return false;
    }

    default boolean cqlStaticColumn() {
        return false;
    }

    default int cqlPrimaryKeyOrder() {
        return -1;
    }

    default boolean cqlClusteringKeyDesc() {
        return false;
    }

    default CQL3Type CQL3Type() {
        return CQL3Type.Native.TEXT;
    }

    /** UDT name override from mapping; forked {@code ObjectMapper} supplies this. */
    default String cqlUdtName() {
        return null;
    }

    /** Cassandra column name as bytes; implemented by {@link Mapper#cqlName()}. */
    ByteBuffer cqlName();

    default CQL3Type.Raw collection(CQL3Type.Raw rawType) {
        if (rawType != null && rawType.supportsFreezing() && !rawType.isFrozen()) {
            rawType = rawType.freeze();
        }
        switch (cqlCollection()) {
            case LIST:
                return CQL3Type.Raw.list(rawType);
            case SET:
                return CQL3Type.Raw.set(rawType);
            default:
                return rawType;
        }
    }
}
