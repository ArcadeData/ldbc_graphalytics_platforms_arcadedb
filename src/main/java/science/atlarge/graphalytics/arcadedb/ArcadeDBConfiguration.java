/*
 * Copyright 2015 - 2026 Delft University of Technology / Arcade Data Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.arcadedb;

import org.apache.commons.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.configuration.ConfigurationUtil;
import science.atlarge.graphalytics.configuration.GraphalyticsExecutionException;

import java.nio.file.Paths;

/**
 * Collection of configurable platform options for ArcadeDB.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ArcadeDBConfiguration {

    protected static final Logger LOG = LogManager.getLogger();

    private static final String BENCHMARK_PROPERTIES_FILE = "benchmark.properties";
    private static final String HOME_PATH_KEY = "platform.arcadedb.home";

    private String loaderPath;
    private String unloaderPath;
    private String homePath;

    public ArcadeDBConfiguration() {
    }

    public String getLoaderPath() {
        return loaderPath;
    }

    public void setLoaderPath(String loaderPath) {
        this.loaderPath = loaderPath;
    }

    public String getUnloaderPath() {
        return unloaderPath;
    }

    public void setUnloaderPath(String unloaderPath) {
        this.unloaderPath = unloaderPath;
    }

    public String getHomePath() {
        return homePath;
    }

    public void setHomePath(String homePath) {
        this.homePath = homePath;
    }

    public static ArcadeDBConfiguration parsePropertiesFile() {

        ArcadeDBConfiguration platformConfig = new ArcadeDBConfiguration();

        Configuration configuration = null;
        try {
            configuration = ConfigurationUtil.loadConfiguration(BENCHMARK_PROPERTIES_FILE);
        } catch (Exception e) {
            LOG.warn(String.format("Failed to load configuration from %s", BENCHMARK_PROPERTIES_FILE));
            throw new GraphalyticsExecutionException("Failed to load configuration. Benchmark run aborted.", e);
        }

        String loaderPath = Paths.get("./bin/sh/load-graph.sh").toString();
        platformConfig.setLoaderPath(loaderPath);

        String unloaderPath = Paths.get("./bin/sh/unload-graph.sh").toString();
        platformConfig.setUnloaderPath(unloaderPath);

        String homePath = configuration.getString(HOME_PATH_KEY, null);
        if (homePath != null) {
            platformConfig.setHomePath(homePath);
        }

        return platformConfig;
    }

}
