package com.ireport.twitter.storm.topology.spouts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterPublicTopicsQuerySpout extends BaseRichSpout {
	private static final Logger logger = LoggerFactory
			.getLogger(TwitterPublicTopicsQuerySpout.class);

	private static final int CAPACITY = 1000;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1853277510098146839L;
	private SpoutOutputCollector collector;
	private Twitter twitter;
	private Query query;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		ConfigurationBuilder cb = new ConfigurationBuilder();
		TwitterFactory tf = new TwitterFactory(cb.build());
		twitter = tf.getInstance();
		String keyword = "(politics) OR (worst) OR (not good) OR (pathetic)  OR (poor service) OR (Govt) OR (country) OR (Government) ";
		query = new Query(
				keyword
						+ " -filter:retweets -filter:links -filter:replies -filter:images");
		query.setLocale("en");
		query.setLang("en");
		query.setCount(100);
//		query.since("");
	}

	@Override
	public void nextTuple() {
		try {
			QueryResult queryResult = twitter.search(query);
			for (Status s : queryResult.getTweets()) {
				collector.emit(new Values(" Geo: " + s.getGeoLocation()
						+ " Tweet: " + s.getText()));
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		twitter.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
