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
package science.atlarge.graphalytics.arcadedb.metrics.lcc;

import org.neo4j.driver.Session;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConfiguration;
import science.atlarge.graphalytics.arcadedb.ArcadeDBDatabase;
import science.atlarge.graphalytics.arcadedb.ArcadeDBJob;
import science.atlarge.graphalytics.arcadedb.ProcTimeLog;
import science.atlarge.graphalytics.arcadedb.metrics.OutputSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * ArcadeDB job configuration for calculating the local clustering coefficient.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalClusteringCoefficientJob extends ArcadeDBJob {

    private Map<Long, Number> results;

    public LocalClusteringCoefficientJob(RunSpecification runSpecification, ArcadeDBConfiguration platformConfig,
                                         String inputPath, String outputPath) {
        super(runSpecification, platformConfig, inputPath, outputPath);
    }

    @Override
    public void compute(ArcadeDBDatabase database, Graph graph) {
        try (Session session = database.getSession()) {
            ProcTimeLog.start();
            LocalClusteringCoefficientComputation computation = new LocalClusteringCoefficientComputation(
                    session,
                    graph.isDirected()
            );
            results = computation.run();
            ProcTimeLog.end();
        }
    }

    @Override
    protected void serialize(ArcadeDBDatabase database, String outputPath) throws IOException {
        OutputSerializer.serialize(results, outputPath, true);
    }
}
