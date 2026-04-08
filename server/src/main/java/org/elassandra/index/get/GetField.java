/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 */

package org.elassandra.index.get;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Minimal stored-field list for Elassandra query flattening (OpenSearch removed {@code org.opensearch.index.get.GetField}).
 */
public class GetField implements Iterable<Object> {

    private final String name;
    private final List<Object> values;

    public GetField(String name, List<Object> values) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }
}
