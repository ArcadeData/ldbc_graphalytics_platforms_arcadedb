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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.Session;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Base class for all jobs in the ArcadeDB platform driver. Configures and executes a platform job
 * using the parameters and executable specified by the subclass for a specific algorithm.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class ArcadeDBJob {

    private static final Logger LOG = LogManager.getLogger();

    private final String jobId;
    private final String logPath;
    private final String inputPath;
    private final String outputPath;

    private final Graph graph;
    private final ArcadeDBDatabase database;

    /**
     * Initializes the platform job with its parameters.
     *
     * @param runSpecification the benchmark run specification
     * @param platformConfig   the platform configuration
     * @param inputPath        the path of the loaded graph
     * @param outputPath       the file path of the output
     */
    public ArcadeDBJob(RunSpecification runSpecification,
                       ArcadeDBConfiguration platformConfig,
                       String inputPath,
                       String outputPath) {

        BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
        BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();

        this.jobId = benchmarkRun.getId();
        this.logPath = benchmarkRunSetup.getLogDir().resolve("platform").toString();

        this.inputPath = inputPath;
        this.outputPath = outputPath;

        this.graph = benchmarkRun.getGraph();

        // Read the database name from the loaded graph path
        String databaseName = readDatabaseName(inputPath);

        LOG.info("Connecting to ArcadeDB database: " + databaseName + " at " + platformConfig.getBoltUri());
        this.database = new ArcadeDBDatabase(
                platformConfig.getBoltUri(),
                databaseName,
                platformConfig.getUsername(),
                platformConfig.getPassword()
        );
    }

    private String readDatabaseName(String inputPath) {
        try {
            java.nio.file.Path dbNameFile = Paths.get(inputPath).resolve("database_name");
            if (Files.exists(dbNameFile)) {
                return new String(Files.readAllBytes(dbNameFile)).trim();
            }
        } catch (IOException e) {
            LOG.warn("Failed to read database name file, deriving from path", e);
        }
        // Fallback: derive from path
        String name = Paths.get(inputPath).getFileName().toString();
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    /**
     * Executes the platform job with the pre-defined parameters.
     *
     * @return the exit code
     */
    public int execute() throws IOException {
        try {
            compute(database, graph);
            serialize(database, outputPath);
        } finally {
            database.close();
        }
        return 0;
    }

    protected abstract void compute(
            ArcadeDBDatabase database,
            Graph graph
    ) throws IOException;

    protected void serialize(
            ArcadeDBDatabase database,
            String outputPath
    ) throws IOException { }

}
