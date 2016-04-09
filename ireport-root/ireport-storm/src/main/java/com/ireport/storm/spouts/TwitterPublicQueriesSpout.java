package com.ireport.storm.spouts;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings("serial")
public class TwitterPublicQueriesSpout extends BaseRichSpout {

	private static final int CAPACITY = 1000;

	SpoutOutputCollector _collector;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	double[][] loc;
	private Twitter twitter;
	private Query query;

	public TwitterPublicQueriesSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret, String[] keyWords,
			double[][] loc) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		this.loc = loc;
	}

	public TwitterPublicQueriesSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this._collector = collector;
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
		// query.since("");
	}

	@Override
	public void nextTuple() {
		try {
			QueryResult queryResult = twitter.search(query);
			for (Status s : queryResult.getTweets()) {
				_collector.emit(new Values(" Geo: " + s.getGeoLocation()
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
