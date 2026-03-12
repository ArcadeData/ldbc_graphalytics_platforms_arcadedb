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
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.execution.*;
import science.atlarge.graphalytics.arcadedb.metrics.ArcadeDBJobFactory;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * ArcadeDB platform driver for the LDBC Graphalytics benchmark.
 * Uses ArcadeDB in embedded mode with native graph algorithms.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadedbPlatform implements Platform {

	protected static final Logger LOG = LogManager.getLogger();
	private static final String PLATFORM_NAME = "arcadedb";

	public ArcadeDBLoader loader;

	@Override
	public void verifySetup() { }

	@Override
	public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
		ArcadeDBConfiguration platformConfig = ArcadeDBConfiguration.parsePropertiesFile();
		loader = new ArcadeDBLoader(formattedGraph, platformConfig);

		LOG.info("Loading graph " + formattedGraph.getName());
		Path loadedPath = Paths.get("./intermediate").resolve(formattedGraph.getName());
		String databasePath = loadedPath.resolve("database").toString();

		try {
			int exitCode = loader.load(databasePath);
			if (exitCode != 0) {
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to load an ArcadeDB dataset.", e);
		}
		LOG.info("Loaded graph " + formattedGraph.getName());

		return new LoadedGraph(formattedGraph, databasePath);
	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) throws Exception {
		LOG.info("Unloading graph " + loadedGraph.getFormattedGraph().getName());
		try {
			int exitCode = loader.unload(loadedGraph.getLoadedPath());
			if (exitCode != 0) {
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to unload an ArcadeDB dataset.", e);
		}
		LOG.info("Unloaded graph " + loadedGraph.getFormattedGraph().getName());
	}

	@Override
	public void prepare(RunSpecification runSpecification) { }

	@Override
	public void startup(RunSpecification runSpecification) {
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		Path logDir = benchmarkRunSetup.getLogDir().resolve("platform").resolve("runner.logs");
		ArcadeDBCollector.startPlatformLogging(logDir);
	}

	@Override
	public void run(RunSpecification runSpecification) throws PlatformExecutionException {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		RuntimeSetup runtimeSetup = runSpecification.getRuntimeSetup();

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		ArcadeDBConfiguration platformConfig = ArcadeDBConfiguration.parsePropertiesFile();
		String inputPath = runtimeSetup.getLoadedGraph().getLoadedPath();
		String outputPath = benchmarkRunSetup.getOutputDir().resolve(benchmarkRun.getName()).toAbsolutePath().toString();

		ArcadeDBJobFactory jobFactory = new ArcadeDBJobFactory(
				runSpecification, platformConfig, inputPath, outputPath
		);

		ArcadeDBJob job;
		switch (algorithm) {
			case BFS:
				job = jobFactory.createBfsJob();
				break;
			case CDLP:
				job = jobFactory.createCdlpJob();
				break;
			case LCC:
				job = jobFactory.createLccJob();
				break;
			case PR:
				job = jobFactory.createPrJob();
				break;
			case WCC:
				job = jobFactory.createWccJob();
				break;
			case SSSP:
				job = jobFactory.createSsspJob();
				break;
			default:
				throw new PlatformExecutionException("Failed to load algorithm implementation.");
		}

		LOG.info("Executing benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());

		try {
			int exitCode = job.execute();
			if (exitCode != 0) {
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to execute an ArcadeDB job.", e);
		}

		LOG.info("Executed benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());
	}

	@Override
	public BenchmarkMetrics finalize(RunSpecification runSpecification) throws Exception {
		ArcadeDBCollector.stopPlatformLogging();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		Path logDir = benchmarkRunSetup.getLogDir().resolve("platform");

		BenchmarkMetrics metrics = new BenchmarkMetrics();
		metrics.setProcessingTime(ArcadeDBCollector.collectProcessingTime(logDir));
		return metrics;
	}

	@Override
	public void terminate(RunSpecification runSpecification) {
		BenchmarkRunner.terminatePlatform(runSpecification);
	}

	@Override
	public String getPlatformName() {
		return PLATFORM_NAME;
	}
}
