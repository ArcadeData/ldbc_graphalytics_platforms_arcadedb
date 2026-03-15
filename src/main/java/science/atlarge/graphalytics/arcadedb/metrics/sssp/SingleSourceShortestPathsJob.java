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
package science.atlarge.graphalytics.arcadedb.metrics.sssp;

import com.arcadedb.database.Database;
import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConfiguration;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConstants;
import science.atlarge.graphalytics.arcadedb.ArcadeDBJob;
import science.atlarge.graphalytics.arcadedb.ProcTimeLog;
import science.atlarge.graphalytics.arcadedb.metrics.OutputSerializer;

import java.io.IOException;

/**
 * ArcadeDB job configuration for executing the single source shortest paths algorithm.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SingleSourceShortestPathsJob extends ArcadeDBJob {

    private final SingleSourceShortestPathsParameters parameters;

    public SingleSourceShortestPathsJob(RunSpecification runSpecification, ArcadeDBConfiguration platformConfig,
                                        String inputPath, String outputPath) {
        super(runSpecification, platformConfig, inputPath, outputPath);
        this.parameters = (SingleSourceShortestPathsParameters) runSpecification
                .getBenchmarkRun()
                .getAlgorithmParameters();
    }

    @Override
    public void compute(Database graphDatabase, Graph graph) {
        ProcTimeLog.start();
        SingleSourceShortestPathsComputation computation = new SingleSourceShortestPathsComputation(
                graphDatabase,
                parameters.getSourceVertex(),
                graph.isDirected()
        );
        computation.run();
        ProcTimeLog.end();
    }

    @Override
    protected void serialize(Database graphDatabase, String outputPath) throws IOException {
        OutputSerializer<Double> serializer = new OutputSerializer<>(
                ArcadeDBConstants.SSSP,
                Double.POSITIVE_INFINITY
        );
        serializer.serialize(graphDatabase, outputPath);
    }
}
