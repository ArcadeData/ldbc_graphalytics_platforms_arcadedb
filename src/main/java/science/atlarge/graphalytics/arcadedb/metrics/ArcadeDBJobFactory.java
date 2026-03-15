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
 * Factory for creating ArcadeDB benchmark jobs. Uses ArcadeDB's native graph
 * algorithms invoked via Cypher procedures over the Bolt protocol.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBJobFactory {

    protected RunSpecification runSpecification;
    protected ArcadeDBConfiguration platformConfig;
    protected String inputPath;
    protected String outputPath;

    public ArcadeDBJobFactory(RunSpecification runSpecification, ArcadeDBConfiguration platformConfig,
                              String inputPath, String outputPath) {
        this.runSpecification = runSpecification;
        this.platformConfig = platformConfig;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public ArcadeDBJob createBfsJob() {
        return new BreadthFirstSearchJob(runSpecification, platformConfig, inputPath, outputPath);
    }

    public ArcadeDBJob createCdlpJob() {
        return new CommunityDetectionLPJob(runSpecification, platformConfig, inputPath, outputPath);
    }

    public ArcadeDBJob createLccJob() {
        return new LocalClusteringCoefficientJob(runSpecification, platformConfig, inputPath, outputPath);
    }

    public ArcadeDBJob createPrJob() {
        return new PageRankJob(runSpecification, platformConfig, inputPath, outputPath);
    }

    public ArcadeDBJob createWccJob() {
        return new WeaklyConnectedComponentsJob(runSpecification, platformConfig, inputPath, outputPath);
    }

    public ArcadeDBJob createSsspJob() {
        return new SingleSourceShortestPathsJob(runSpecification, platformConfig, inputPath, outputPath);
    }
}
