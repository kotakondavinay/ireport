package com.ireport.twitter.storm.topology.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterTopicClassifierBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory
			.getLogger(TwitterTopicClassifierBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -6584626469589835291L;
	private OutputCollector collector;

	public TwitterTopicClassifierBolt() {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String tweet = (String) input.getValueByField("tweet");
		String text = tweet.replaceAll("\\p{Punct}", " ").toLowerCase();
		collector.emit(new Values(text));
		logger.info("Tweet: " + text);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text"));
	}

}
