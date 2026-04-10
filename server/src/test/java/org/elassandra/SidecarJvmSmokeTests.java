package org.elassandra;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Minimal JVM/bootstrap check for OpenSearch side-car (no cluster settings mutations).
 */
@ThreadLeakScope(Scope.NONE)
public class SidecarJvmSmokeTests extends OpenSearchSingleNodeTestCase {

    @Test
    public void cassandraAndOpensearchBootstrapped() {
        assertTrue(client() != null);
    }
}
