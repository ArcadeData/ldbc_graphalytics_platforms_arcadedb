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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.io.IOException;

/**
 * Base class for all jobs in the ArcadeDB platform driver. The database is
 * shared across all algorithm runs (managed by ArcadedbPlatform) and the GAV
 * is built once during graph loading.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class ArcadeDBJob {

    private static final Logger LOG = LogManager.getLogger();

    private final String jobId;
    private final String logPath;
    private final String outputPath;

    private final Graph graph;
    private final Database database;

    /**
     * Initializes the platform job with a shared database instance.
     *
     * @param runSpecification the benchmark run specification
     * @param platformConfig   the platform configuration
     * @param database         the shared database instance (managed by ArcadedbPlatform)
     * @param outputPath       the file path of the output graph dataset
     */
    public ArcadeDBJob(RunSpecification runSpecification,
                       ArcadeDBConfiguration platformConfig,
                       Database database,
                       String outputPath) {

        BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
        BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();

        this.jobId = benchmarkRun.getId();
        this.logPath = benchmarkRunSetup.getLogDir().resolve("platform").toString();

        this.outputPath = outputPath;

        this.graph = benchmarkRun.getGraph();
        this.database = database;
    }

    /**
     * Executes the platform job. The database is NOT closed here — it is
     * managed by ArcadedbPlatform and reused across all algorithm runs.
     *
     * @return the exit code
     */
    public int execute() throws IOException {
        compute(database, graph);
        serialize(database, outputPath);
        return 0;
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
