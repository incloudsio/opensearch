/*
 * Lucene 7/8 vs 9 Weight creation compatibility for Elassandra sources compiled against multiple Lucene lines.
 */
package org.elassandra.index.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.lang.reflect.Method;

public final class LuceneWeights {
    private LuceneWeights() {}

    public static Weight create(IndexSearcher searcher, Query query, org.apache.lucene.index.IndexReader reader) throws IOException {
        try {
            Method m = IndexSearcher.class.getMethod("createWeight", Query.class, ScoreMode.class, float.class);
            Query rewritten = query.rewrite(reader);
            return (Weight) m.invoke(searcher, rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        } catch (NoSuchMethodException e) {
            try {
                Method m = IndexSearcher.class.getMethod("createNormalizedWeight", Query.class, boolean.class);
                return (Weight) m.invoke(searcher, query, false);
            } catch (ReflectiveOperationException e2) {
                throw new IOException(e2);
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }
}
