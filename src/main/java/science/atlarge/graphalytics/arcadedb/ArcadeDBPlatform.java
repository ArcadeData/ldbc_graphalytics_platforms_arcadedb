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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewBuilder;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
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
import java.util.concurrent.TimeUnit;

/**
 * ArcadeDB platform driver for the LDBC Graphalytics benchmark.
 * Uses ArcadeDB in embedded mode with native graph algorithms.
 * <p>
 * When OLAP mode is enabled, the Graph Analytical View (GAV) is built during
 * the loadGraph phase (counted as load_time) and reused across all algorithm runs,
 * eliminating the overhead of rebuilding the CSR for each algorithm.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadedbPlatform implements Platform {

	protected static final Logger LOG = LogManager.getLogger();
	private static final String PLATFORM_NAME = "arcadedb";

	public ArcadeDBLoader loader;
	private ArcadeDBConfiguration platformConfig;
	private Database database;
	private String databasePath;
	private int jobCounter = 0;
	private int totalJobs = 0;

	@Override
	public void verifySetup() { }

	@Override
	public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
		platformConfig = ArcadeDBConfiguration.parsePropertiesFile();
		loader = new ArcadeDBLoader(formattedGraph, platformConfig);

		LOG.info("Loading graph " + formattedGraph.getName());
		Path loadedPath = Paths.get("./intermediate").resolve(formattedGraph.getName());
		databasePath = loadedPath.resolve("database").toString();

		try {
			int exitCode = loader.load(databasePath);
			if (exitCode != 0)
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to load an ArcadeDB dataset.", e);
		}

		// Open database and keep it open for all algorithm runs
		LOG.info("Opening database for benchmark runs: " + databasePath);
		database = new DatabaseFactory(databasePath).open();

		// Build GAV during load phase (counted as load_time, not processing_time)
		if (platformConfig.isOlap()) {
			buildGraphOLAP(database, formattedGraph.hasEdgeProperties());
		}

		LOG.info("Loaded graph " + formattedGraph.getName());
		return new LoadedGraph(formattedGraph, databasePath);
	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) throws Exception {
		LOG.info("Unloading graph " + loadedGraph.getFormattedGraph().getName());

		// Close the shared database
		if (database != null) {
			// Drop GAV if present
			try {
				GraphAnalyticalView gav = GraphAnalyticalViewRegistry.get(database, "benchmark");
				if (gav != null)
					gav.drop();
			} catch (Exception e) {
				LOG.warn("Failed to drop GAV: {}", e.getMessage());
			}
			database.close();
			database = null;
		}

		try {
			int exitCode = loader.unload(loadedGraph.getLoadedPath());
			if (exitCode != 0)
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
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

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		String outputPath = benchmarkRunSetup.getOutputDir().resolve(benchmarkRun.getName()).toAbsolutePath().toString();

		ArcadeDBJobFactory jobFactory = new ArcadeDBJobFactory(
				runSpecification, platformConfig, database, outputPath
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

		jobCounter++;
		if (totalJobs == 0) totalJobs = 6; // default for custom benchmark

		LOG.info("=== [Job {}/{}] Running {} on graph \"{}\" ===",
				jobCounter, totalJobs,
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());

		try {
			long startTime = System.currentTimeMillis();
			int exitCode = job.execute();
			long elapsed = System.currentTimeMillis() - startTime;
			if (exitCode != 0)
				throw new PlatformExecutionException("ArcadeDB exited with an error code: " + exitCode);
			LOG.info("=== [Job {}/{}] Completed {} in {}.{}s ===",
					jobCounter, totalJobs,
					benchmarkRun.getAlgorithm().getName(),
					elapsed / 1000, String.format("%03d", elapsed % 1000));
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to execute an ArcadeDB job.", e);
		}
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

	private void buildGraphOLAP(Database db, boolean hasEdgeProperties) {
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
					return;
				}

				// Restored GAV failed — drop it and build fresh
				LOG.warn("Restored Graph Analytical View failed (status: {}), dropping and rebuilding...",
						gav.getStatus());
				try {
					gav.drop();
				} catch (Exception dropEx) {
					LOG.warn("Failed to drop failed GAV, forcing unregister: {}", dropEx.getMessage());
					GraphAnalyticalViewRegistry.unregister(db, "benchmark");
				}
			}

			// Build a new GAV
			LOG.info("Building Graph Analytical View (OLAP mode enabled)...");
			GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(db)
					.withName("benchmark")
					.withVertexTypes(ArcadeDBConstants.VERTEX_TYPE)
					.withEdgeTypes(ArcadeDBConstants.EDGE_TYPE);
			if (hasEdgeProperties)
				builder.withEdgeProperties(ArcadeDBConstants.WEIGHT_PROPERTY);
			gav = builder.build();

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
}
