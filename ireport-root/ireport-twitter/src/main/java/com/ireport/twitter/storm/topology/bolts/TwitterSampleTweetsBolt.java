package com.ireport.twitter.storm.topology.bolts;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterSampleTweetsBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory
			.getLogger(TwitterSampleTweetsBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = -6584626469589835291L;
	private OutputCollector collector;

	public TwitterSampleTweetsBolt() {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		String lang = tweet.getUser().getLang();
		String text = tweet.getText().replaceAll("\\p{Punct}", " ")
				.toLowerCase();
		collector.emit(new Values(lang, text));
		logger.info("Tweet: " + text);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lang", "text"));
	}

}
