/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra.index.search;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.shard.IndexSearcherWrapper;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

public class TokenRangesSearcherWrapper extends IndexSearcherWrapper {

    private static ThreadLocal<ShardSearchRequest> shardSearchRequest = new ThreadLocal<>();

    public static void current(ShardSearchRequest value) {
        shardSearchRequest.set(value);
    }

    public static void removeCurrent() {
        shardSearchRequest.remove();
    }

    public static ShardSearchRequest current() {
        return shardSearchRequest.get();
    }

    private final TokenRangesBitsetFilterCache filterCache;
    private final TokenRangesService tokenRangesService;

    public TokenRangesSearcherWrapper(TokenRangesBitsetFilterCache filterCache, TokenRangesService tokenRangeService) {
        this.filterCache = filterCache;
        this.tokenRangesService = tokenRangeService;
    }

    @Override
    public final DirectoryReader wrap(final DirectoryReader in) throws IOException {
        // OpenSearch 1.3: ShardSearchRequest does not yet carry Cassandra token-ranges; restore TokenRangesDirectoryReader when SearchRequest is extended.
        return in;
    }

    public Query query(BooleanQuery.Builder qb) {
        return qb.build();
    }

}
