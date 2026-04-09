/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elassandra.env;

import org.opensearch.cli.Terminal;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.InternalSettingsPreparer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Elasticsearch file configuration loader interface.
 */
public interface EnvironmentLoader {

    default Environment loadEnvironment(boolean foreground, String homeDir, String configDir) {
        final Settings settings = Settings.builder()
            .put("node.name", "node0")
            .put("path.home", homeDir)
            .build();
        final Path cfg = Paths.get(configDir);
        try {
            java.lang.reflect.Method m = InternalSettingsPreparer.class.getMethod(
                "prepareEnvironment",
                Settings.class,
                Map.class,
                Path.class,
                java.util.function.Supplier.class
            );
            return (Environment) m.invoke(
                null,
                settings,
                Collections.<String, String>emptyMap(),
                cfg,
                (java.util.function.Supplier<String>) () -> "node0"
            );
        } catch (NoSuchMethodException e) {
            try {
                java.lang.reflect.Method m2 = InternalSettingsPreparer.class.getMethod(
                    "prepareEnvironment",
                    Settings.class,
                    Terminal.class,
                    Map.class,
                    Path.class
                );
                return (Environment) m2.invoke(
                    null,
                    settings,
                    foreground ? Terminal.DEFAULT : null,
                    Collections.<String, String>emptyMap(),
                    cfg
                );
            } catch (ReflectiveOperationException e2) {
                throw new RuntimeException(e2);
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
