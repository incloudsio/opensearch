/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 */

package org.elassandra.cluster;

import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parsed Cassandra primary key for a document id / routing string (was {@code ClusterService.DocPrimaryKey}).
 */
public class DocPrimaryKey {
    public String[] names;
    public Object[] values;
    /** pk = partition key and pk has clustering key. */
    public boolean isStaticDocument;

    public DocPrimaryKey(String[] names, Object[] values, boolean isStaticDocument) {
        this.names = names;
        this.values = values;
        this.isStaticDocument = isStaticDocument;
    }

    public DocPrimaryKey(String[] names, Object[] values) {
        this.names = names;
        this.values = values;
        this.isStaticDocument = false;
    }

    public List<ByteBuffer> serialize(QueryHandler.Prepared prepared) {
        List<ByteBuffer> boundValues = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++) {
            Object v = values[i];
            AbstractType<?> type = prepared.statement.getBindVariables().get(i).type;
            if (v instanceof ByteBuffer || v == null) {
                boundValues.add((ByteBuffer) v);
            } else {
                @SuppressWarnings("unchecked")
                AbstractType<Object> ot = (AbstractType<Object>) type;
                boundValues.add(ot.decompose(v));
            }
        }
        return boundValues;
    }

    @Override
    public String toString() {
        return Serializer.stringify(values, values.length);
    }
}
