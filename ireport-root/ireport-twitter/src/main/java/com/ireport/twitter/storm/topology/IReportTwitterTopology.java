package com.ireport.twitter.storm.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ireport.twitter.storm.topology.bolts.TwitterSampleTweetsBolt;
import com.ireport.twitter.storm.topology.spouts.TwitterPublicTopicsSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class IReportTwitterTopology {
	private static final Logger logger = LoggerFactory
			.getLogger(IReportTwitterTopology.class);
	private static final String IREPORT_TWITTER_TOPO = "IREPORT_TWITTER_TOPO";

	public static void main(String args[]) {
		Config conf = new Config();
		conf.setMessageTimeoutSecs(120);
		TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout("TwitterPublicTopicsSpout",
				new TwitterPublicTopicsSpout());
		topology.setBolt("TwitterSampleTweetsBolt",
				new TwitterSampleTweetsBolt()).shuffleGrouping(
				"TwitterPublicTopicsSpout");

		final LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(IREPORT_TWITTER_TOPO, conf,
				topology.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				localCluster.killTopology(IREPORT_TWITTER_TOPO);
				localCluster.shutdown();
			}
		});
	}
}
