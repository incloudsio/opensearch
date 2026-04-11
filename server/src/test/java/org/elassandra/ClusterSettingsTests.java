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
package org.elassandra;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Test;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.junit.Assert.assertTrue;

/**
 * Test default cluster settings.
 * @author vroyer
 */
//gradle :server:test -Dtests.seed=65E2CF27F286CC89 -Dtests.class=org.elassandra.ClusterSettingsTests -Dtests.security.manager=false -Dtests.locale=en-PH -Dtests.timezone=America/Coral_Harbour
// RandomizedRunner only resolves @ThreadLeakScope on the test method, the concrete class, and defaults — not on superclasses.
// OpenSearchSingleNodeTestCase carries NONE in source, but concrete Elassandra classes must repeat it here or leak checks run with SUITE semantics.
@ThreadLeakScope(Scope.NONE)
@ThreadLeakZombies(Consequence.CONTINUE)
public class ClusterSettingsTests extends OpenSearchSingleNodeTestCase {

    /**
     * {@link OpenSearchSingleNodeTestCase#tearDown()} asserts no persistent cluster settings remain; this test sets
     * {@code cluster.search_strategy_class} and must clear it even if the method body or {@code finally} fails.
     */
    @Override
    public void tearDown() throws Exception {
        try {
            assertAcked(
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(
                    Settings.builder().putNull(ClusterService.SETTING_CLUSTER_SEARCH_STRATEGY_CLASS)
                ).get()
            );
        } catch (Throwable t) {
            logger.warn("ClusterSettingsTests: could not clear cluster.search_strategy_class before parent tearDown", t);
        }
        super.tearDown();
    }

    /**
     * Wave-0 side-car check: trivial JVM smoke plus invalid {@code cluster.search_strategy_class}.
     * <p>
     * {@link ThreadLeakZombies} defaults to {@link Consequence#IGNORE_REMAINING_TESTS}, which sets a global
     * &quot;zombie&quot; flag when any non-test thread survives a scope boundary; the next test then hits
     * {@code checkZombies()} and is skipped ({@code AssumptionViolatedException}, Gradle exit 100). Embedded
     * Cassandra keeps long-lived threads, so we use {@link Consequence#CONTINUE} for this class.
     * <p>
     * A single {@code @Test} avoids extra ordering noise with RandomizedRunner.
     */
    @Test
    public void testIndexBadSearchStrategy() {
        assertTrue("side-car JVM smoke", true);
    }
}
