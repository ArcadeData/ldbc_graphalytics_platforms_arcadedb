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
    private static final String BOLT_URI_KEY = "platform.arcadedb.bolt-uri";
    private static final String HTTP_URI_KEY = "platform.arcadedb.http-uri";
    private static final String USERNAME_KEY = "platform.arcadedb.username";
    private static final String PASSWORD_KEY = "platform.arcadedb.password";

    private String loaderPath;
    private String unloaderPath;
    private String homePath;
    private String boltUri;
    private String httpUri;
    private String username;
    private String password;

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

    public String getBoltUri() {
        return boltUri;
    }

    public void setBoltUri(String boltUri) {
        this.boltUri = boltUri;
    }

    public String getHttpUri() {
        return httpUri;
    }

    public void setHttpUri(String httpUri) {
        this.httpUri = httpUri;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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

        String boltUri = configuration.getString(BOLT_URI_KEY, "bolt://localhost:7687");
        platformConfig.setBoltUri(boltUri);

        String httpUri = configuration.getString(HTTP_URI_KEY, "http://localhost:2480");
        platformConfig.setHttpUri(httpUri);

        String username = configuration.getString(USERNAME_KEY, "root");
        platformConfig.setUsername(username);

        String password = configuration.getString(PASSWORD_KEY, "playwithdata");
        platformConfig.setPassword(password);

        return platformConfig;
    }

}
