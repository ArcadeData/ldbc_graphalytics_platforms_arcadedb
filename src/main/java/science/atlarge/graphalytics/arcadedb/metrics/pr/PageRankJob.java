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
package science.atlarge.graphalytics.arcadedb.metrics.pr;

import com.arcadedb.database.Database;
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConfiguration;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConstants;
import science.atlarge.graphalytics.arcadedb.ArcadeDBJob;
import science.atlarge.graphalytics.arcadedb.ProcTimeLog;
import science.atlarge.graphalytics.arcadedb.metrics.OutputSerializer;

import com.arcadedb.database.Database;

import java.io.IOException;

/**
 * ArcadeDB job configuration for calculating PageRank values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageRankJob extends ArcadeDBJob {

    private final PageRankParameters parameters;

    public PageRankJob(RunSpecification runSpecification, ArcadeDBConfiguration platformConfig,
                       Database database, String outputPath) {
        super(runSpecification, platformConfig, database, outputPath);
        this.parameters = (PageRankParameters) runSpecification
                .getBenchmarkRun()
                .getAlgorithmParameters();
    }

    @Override
    public void compute(Database graphDatabase, Graph graph) {
        ProcTimeLog.start();
        PageRankComputation computation = new PageRankComputation(
                graphDatabase,
                parameters.getNumberOfIterations(),
                parameters.getDampingFactor(),
                graph.isDirected()
        );
        computation.run();
        ProcTimeLog.end();
    }

    @Override
    protected void serialize(Database graphDatabase, String outputPath) throws IOException {
        OutputSerializer<Double> serializer = new OutputSerializer<>(
                ArcadeDBConstants.PAGERANK,
                null
        );
        serializer.serialize(graphDatabase, outputPath);
    }
}
