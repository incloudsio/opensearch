/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 */

package org.opensearch.index.engine;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesReference;

/**
 * Delete-by-query operation (extracted from {@link Engine} for reuse / OpenSearch side-car overlay).
 */
public class DeleteByQuery {
    private final Query query;
    private final BytesReference source;
    private final String[] filteringAliases;
    private final Query aliasFilter;
    private final String[] types;
    private final BitSetProducer parentFilter;
    private final Engine.Operation.Origin origin;

    private final long startTime;
    private long endTime;

    public DeleteByQuery(
        Query query,
        BytesReference source,
        @Nullable String[] filteringAliases,
        @Nullable Query aliasFilter,
        BitSetProducer parentFilter,
        Engine.Operation.Origin origin,
        long startTime,
        String... types
    ) {
        this.query = query;
        this.source = source;
        this.types = types;
        this.filteringAliases = filteringAliases;
        this.aliasFilter = aliasFilter;
        this.parentFilter = parentFilter;
        this.startTime = startTime;
        this.origin = origin;
    }

    public Query query() {
        return this.query;
    }

    public BytesReference source() {
        return this.source;
    }

    public String[] types() {
        return this.types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public Query aliasFilter() {
        return aliasFilter;
    }

    public boolean nested() {
        return parentFilter != null;
    }

    public BitSetProducer parentFilter() {
        return parentFilter;
    }

    public Engine.Operation.Origin origin() {
        return this.origin;
    }

    public long startTime() {
        return this.startTime;
    }

    public DeleteByQuery endTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    public long endTime() {
        return this.endTime;
    }
}
