package com.ireport.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings("serial")
public class TwitterPublicTopicTweetsSpout extends BaseRichSpout {

	private static final int CAPACITY = 1000;
	private boolean isTwitterEnvEnabled = false;
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	double[][] loc;

	public TwitterPublicTopicTweetsSpout(String consumerKey,
			String consumerSecret, String accessToken,
			String accessTokenSecret, String[] keyWords, double[][] loc) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		this.loc = loc;
	}

	public TwitterPublicTopicTweetsSpout() {
		isTwitterEnvEnabled = true;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(CAPACITY);
		_collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {

				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
			}

		};
		_twitterStream = new TwitterStreamFactory(new ConfigurationBuilder()
				.setJSONStoreEnabled(true).build()).getInstance();
		if (!isTwitterEnvEnabled) {
			_twitterStream.addListener(listener);
			_twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
			AccessToken token = new AccessToken(accessToken, accessTokenSecret);
			_twitterStream.setOAuthAccessToken(token);
		}

		if (keyWords.length == 0) {
			_twitterStream.sample();
		} else {
			FilterQuery query = new FilterQuery().track(keyWords);
			query.locations(loc);
			_twitterStream.filter(query);
		}

	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			String sentence = ret.getText();
			System.out.println(" Twitter tweet: " + sentence);
			_collector.emit("streamB", new Values(sentence));
			String delims = "[ .,?!]+";
			String[] tokens = sentence.split(delims);
			for (String token : tokens) {
				_collector.emit("streamA", new Values(token));
			}
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
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
		declarer.declareStream("streamA", new Fields("words"));
		declarer.declareStream("streamB", new Fields("rawmsg"));
	}

}
