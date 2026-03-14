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
package science.atlarge.graphalytics.arcadedb.metrics;

import com.arcadedb.database.Database;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.arcadedb.ArcadeDBConfiguration;
import science.atlarge.graphalytics.arcadedb.ArcadeDBJob;
import science.atlarge.graphalytics.arcadedb.metrics.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.arcadedb.metrics.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.arcadedb.metrics.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.arcadedb.metrics.pr.PageRankJob;
import science.atlarge.graphalytics.arcadedb.metrics.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.arcadedb.metrics.wcc.WeaklyConnectedComponentsJob;

/**
 * Factory for creating ArcadeDB benchmark jobs with a shared database instance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBJobFactory {

    protected RunSpecification runSpecification;
    protected ArcadeDBConfiguration platformConfig;
    protected Database database;
    protected String outputPath;

    public ArcadeDBJobFactory(RunSpecification runSpecification, ArcadeDBConfiguration platformConfig,
                              Database database, String outputPath) {
        this.runSpecification = runSpecification;
        this.platformConfig = platformConfig;
        this.database = database;
        this.outputPath = outputPath;
    }

    public ArcadeDBJob createBfsJob() {
        return new BreadthFirstSearchJob(runSpecification, platformConfig, database, outputPath);
    }

    public ArcadeDBJob createCdlpJob() {
        return new CommunityDetectionLPJob(runSpecification, platformConfig, database, outputPath);
    }

    public ArcadeDBJob createLccJob() {
        return new LocalClusteringCoefficientJob(runSpecification, platformConfig, database, outputPath);
    }

    public ArcadeDBJob createPrJob() {
        return new PageRankJob(runSpecification, platformConfig, database, outputPath);
    }

    public ArcadeDBJob createWccJob() {
        return new WeaklyConnectedComponentsJob(runSpecification, platformConfig, database, outputPath);
    }

    public ArcadeDBJob createSsspJob() {
        return new SingleSourceShortestPathsJob(runSpecification, platformConfig, database, outputPath);
    }
}
