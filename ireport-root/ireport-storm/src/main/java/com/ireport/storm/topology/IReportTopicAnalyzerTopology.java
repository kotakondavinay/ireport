package com.ireport.storm.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.ireport.storm.spouts.TwitterPublicTopicTweetsSpout;

public class IReportTopicAnalyzerTopology {
	private static final Logger LOG = Logger
			.getLogger(IReportTopicAnalyzerTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public IReportTopicAnalyzerTopology(String topologyName, String[] args)
			throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		createTopology(args);
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		return conf;
	}

	private void createTopology(String[] args) throws InterruptedException {
		String spoutId = "twitterSpout";
		String counterId = "counter";
		String intermediateRankerId = "intermediateRanker";
		String totalRankerId = "finalRanker";
		String hdfsBoltId = "hdfsBolt";
		TwitterPublicTopicTweetsSpout spout = null;
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];

		String[] keyWords = { "politics", "problem", "worst", "pathetic",
				"resolve", "poor service", "worst behaviour", "not good",
				"issue", "frustrate", "disgust" };
		double[][] loc = { { -122.75, 36.8 }, { -121.75, 37.8 }, { -74, 40 },
				{ -73, 41 } };
		spout = new TwitterPublicTopicTweetsSpout(consumerKey, consumerSecret,
				accessToken, accessTokenSecret, keyWords, loc);
		builder.setSpout(spoutId, spout, 5);
		builder.setBolt(counterId, new RollingCountBolt(9, 3), 4)
				.shuffleGrouping(spoutId, "streamA");
		builder.setBolt(hdfsBoltId, createHDFSBolt()).shuffleGrouping(spoutId,
				"streamB");
		builder.setBolt(intermediateRankerId,
				new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(
				counterId, new Fields("obj"));
		builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N))
				.globalGrouping(intermediateRankerId);
	}

	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(),
				"local-ireport", topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName,
				topologyConfig);
	}

	public HdfsBolt createHDFSBolt() {
		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f,
				TimedRotationPolicy.TimeUnit.MINUTES);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				"/tmp/foo/").withExtension(".txt");

		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter("|");

		return new HdfsBolt()
				.withFsUrl("hdfs://localhost:9000")
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(format)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy)
				.addRotationAction(
						new MoveFileAction().toDestination("/tmp/dest2/"));
	}

	public static void main(String[] args) throws Exception {
		String topologyName = null;
		boolean runLocally = true;
		if (args.length == 5) {
			topologyName = args[4];
		}
		if (topologyName != null) {
			runLocally = false;
		}

		LOG.info("Topology name: " + topologyName);
		IReportTopicAnalyzerTopology rtw = new IReportTopicAnalyzerTopology(
				topologyName, args);
		if (runLocally) {
			LOG.info("Running in local mode");
			rtw.runLocally();
		} else {
			LOG.info("Running in remote (cluster) mode");
			rtw.runRemotely();
		}
	}
}
