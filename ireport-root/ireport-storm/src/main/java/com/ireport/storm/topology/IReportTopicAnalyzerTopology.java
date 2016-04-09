//package com.ireport.storm.topology;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.ireport.storm.bolts.IntermediateRankBolt;
//import com.ireport.storm.bolts.RollCountBolt;
//import com.ireport.storm.bolts.TotalRankingBolt;
//import com.ireport.storm.bolts.WordSplitter;
//
//import storm.kafka.BrokerHosts;
//import storm.kafka.KafkaSpout;
//import storm.kafka.SpoutConfig;
//import storm.kafka.StringScheme;
//import storm.kafka.ZkHosts;
//import backtype.storm.spout.MultiScheme;
//import backtype.storm.topology.TopologyBuilder;
//
//public class IReportTopicAnalyzerTopology {
//	private static final Logger log = LoggerFactory.getLogger(IReportTopicAnalyzerTopology.class);
//	
//	private TopologyBuilder topologyBuilder;
////	private Config config;
//	
//	
//	public IReportTopicAnalyzerTopology(String topologyName){
//		
//	}
//	
//	public static void main(String[] args) {
//		TopologyBuilder topicAnalyserTopology = new TopologyBuilder();
//		BrokerHosts hosts = new ZkHosts("localhost:2181");
//		SpoutConfig spoutConfig = new SpoutConfig(hosts, "topic_name",
//				"/zkroot_path", "ireportKafkaSpout");
//		spoutConfig.scheme = (MultiScheme) new StringScheme();
//		topicAnalyserTopology.setSpout("kafkaStreamSpout", new KafkaSpout(
//				spoutConfig));
//		topicAnalyserTopology.setBolt("wordSplitterBolt", new WordSplitter())
//				.shuffleGrouping("kafkaStreamSpout");
//		topicAnalyserTopology.setBolt("rollCountBolt", new RollCountBolt())
//				.shuffleGrouping("wordSplitterBolt");
//		topicAnalyserTopology.setBolt("intermediateRankBolt",
//				new IntermediateRankBolt()).shuffleGrouping("rollCountBolt");
//		topicAnalyserTopology.setBolt("totalRankingBolt",
//				new TotalRankingBolt()).shuffleGrouping("intermediateRankBolt");
//		
//	}
//}
