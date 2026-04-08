/*
 * Elassandra side-car stub: OpenSearch 1.3 removed this class; Elassandra still extends it for token-range search wrapping.
 */
package org.opensearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

public class IndexSearcherWrapper {
    protected DirectoryReader wrap(DirectoryReader reader) throws IOException {
        return reader;
    }

    protected IndexSearcher wrap(IndexSearcher searcher) throws IOException {
        return searcher;
    }
}
