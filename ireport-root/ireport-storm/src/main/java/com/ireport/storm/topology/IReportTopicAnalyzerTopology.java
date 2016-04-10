package com.ireport.storm.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.starter.WordCountTopology.SplitSentence;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.RollingCountBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.ireport.storm.spouts.TwitterPublicQueriesSpout;

public class IReportTopicAnalyzerTopology {
	private static final Logger LOG = Logger
			.getLogger(IReportTopicAnalyzerTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public IReportTopicAnalyzerTopology(String topologyName)
			throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		createTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		return conf;
	}

	private void createTopology() throws InterruptedException {
		String spoutId = "twitterSpout";
		String sentenceSplitId = "sentenceSplitterBolt";
		String counterId = "counter";
		String intermediateRankerId = "intermediateRanker";
		String totalRankerId = "finalRanker";
		builder.setSpout(spoutId, new TwitterPublicQueriesSpout(), 5);
		builder.setBolt(sentenceSplitId, new SplitSentence(), 8)
				.shuffleGrouping(spoutId);
		builder.setBolt(counterId, new RollingCountBolt(9, 3), 4)
				.fieldsGrouping(sentenceSplitId, new Fields("word"));
		builder.setBolt(intermediateRankerId,
				new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(
				counterId, new Fields("obj"));
		builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N))
				.globalGrouping(intermediateRankerId);
	}

	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
				topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName,
				topologyConfig);
	}

	public static void main(String[] args) throws Exception {
		String topologyName = "slidingWindowCounts";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}

		LOG.info("Topology name: " + topologyName);
		IReportTopicAnalyzerTopology rtw = new IReportTopicAnalyzerTopology(
				topologyName);
		if (runLocally) {
			LOG.info("Running in local mode");
			rtw.runLocally();
		} else {
			LOG.info("Running in remote (cluster) mode");
			rtw.runRemotely();
		}
	}
}
