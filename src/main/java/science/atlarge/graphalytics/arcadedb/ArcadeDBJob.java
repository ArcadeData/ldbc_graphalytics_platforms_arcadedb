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

import com.arcadedb.database.Database;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all jobs in the ArcadeDB platform driver. Configures and executes
 * a platform job using the parameters and executable specified by the subclass
 * for a specific algorithm.
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
    private final boolean olap;

    /**
     * Initializes the platform job with its parameters.
     *
     * @param runSpecification the benchmark run specification
     * @param platformConfig   the platform configuration
     * @param inputPath        the file path of the input graph dataset
     * @param outputPath       the file path of the output graph dataset
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
        this.olap = platformConfig.isOlap();

        LOG.info("Opening embedded database from path: " + inputPath);
        this.database = new ArcadeDBDatabase(inputPath);
    }

    /**
     * Executes the platform job with the pre-defined parameters.
     *
     * @return the exit code
     */
    public int execute() throws IOException {
        try {
            if (olap) {
                buildGraphOLAP(database.get());
            }
            compute(
                    database.get(),
                    graph
            );
            serialize(
                    database.get(),
                    outputPath
            );
        } finally {
            database.close();
        }
        return 0;
    }

    private void buildGraphOLAP(Database db) {
        long start = System.currentTimeMillis();
        try {
            // Check if GAV was already restored from persistence on database open
            GraphAnalyticalView gav = GraphAnalyticalViewRegistry.get(db, "benchmark");
            if (gav != null) {
                LOG.info("Graph Analytical View 'benchmark' already registered (restored from persistence), waiting for it to be ready...");
                boolean ready = gav.awaitReady(600, TimeUnit.SECONDS);
                long elapsed = System.currentTimeMillis() - start;
                if (ready) {
                    LOG.info("Graph Analytical View ready in {}.{}s - status: {}",
                            elapsed / 1000, String.format("%03d", elapsed % 1000), gav.getStatus());
                    LOG.info(gav.getStats());
                } else {
                    LOG.warn("Graph Analytical View not ready after {}s - status: {}, falling back to OLTP",
                            elapsed / 1000, gav.getStatus());
                }
                return;
            }

            // Build a new GAV (skip persistence since this is a benchmark-only view)
            LOG.info("Building Graph Analytical View (OLAP mode enabled)...");
            gav = GraphAnalyticalView.builder(db)
                    .withName("benchmark")
                    .withVertexTypes(ArcadeDBConstants.VERTEX_TYPE)
                    .withEdgeTypes(ArcadeDBConstants.EDGE_TYPE)
                    .withEdgeProperties(ArcadeDBConstants.WEIGHT_PROPERTY)
                    .build();
            long elapsed = System.currentTimeMillis() - start;
            LOG.info("Graph Analytical View built in {}.{}s - status: {}",
                    elapsed / 1000, String.format("%03d", elapsed % 1000), gav.getStatus());
            LOG.info(gav.getStats());
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            LOG.warn("GraphAnalyticalView build failed after {}s - falling back to OLTP: {}",
                    elapsed / 1000, e.getMessage());
        }
    }

    protected abstract void compute(
            Database graphDatabase,
            Graph graph
    ) throws IOException;

    protected void serialize(
            Database graphDatabase,
            String outputPath
    ) throws IOException { }

}
