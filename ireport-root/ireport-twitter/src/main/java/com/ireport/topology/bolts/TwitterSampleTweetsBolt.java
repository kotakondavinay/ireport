package com.ireport.topology.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
	private FileWriter fileWriter = null;

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
		//collector.emit(new Values(lang, text));
		try {
			this.fileWriter = new FileWriter("/opt/bolt.txt", true);
			Writer output = new BufferedWriter(this.fileWriter);
			output.append(text);
			output.append("\n");
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.info("tweet failed "+text);
			e.printStackTrace();
			collector.fail(input);
		}
		collector.ack(input);
		logger.info("Tweet: " + text);
		logger.info("Tweet: " + text);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lang", "text"));
	}

}
