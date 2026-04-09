/*
 * Lucene 7 vs 8+ Weight creation: Lucene 7 has createNormalizedWeight; Lucene 8+ uses createWeight(Query, ScoreMode, float).
 * ScoreMode exists only from Lucene 8 — do not import it (Elassandra 6.8 ships Lucene 7.7.x on the main build).
 */
package org.elassandra.index.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.lang.reflect.Method;

public final class LuceneWeights {
    private LuceneWeights() {}

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Weight create(IndexSearcher searcher, Query query, org.apache.lucene.index.IndexReader reader) throws IOException {
        Query rewritten = query.rewrite(reader);
        try {
            Class<?> scoreModeClass = Class.forName("org.apache.lucene.search.ScoreMode");
            Object completeNoScores = Enum.valueOf((Class) scoreModeClass, "COMPLETE_NO_SCORES");
            Method m = IndexSearcher.class.getMethod("createWeight", Query.class, scoreModeClass, float.class);
            return (Weight) m.invoke(searcher, rewritten, completeNoScores, 1f);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            try {
                Method m = IndexSearcher.class.getMethod("createNormalizedWeight", Query.class, boolean.class);
                return (Weight) m.invoke(searcher, rewritten, false);
            } catch (ReflectiveOperationException e2) {
                throw new IOException(e2);
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        }
    }
}
