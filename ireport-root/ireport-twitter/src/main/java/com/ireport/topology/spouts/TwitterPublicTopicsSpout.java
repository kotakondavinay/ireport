package com.ireport.topology.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterPublicTopicsSpout extends BaseRichSpout {
	private static final Logger logger = LoggerFactory
			.getLogger(TwitterPublicTopicsSpout.class);

	private static final int CAPACITY = 1000;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1853277510098146839L;
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(CAPACITY);
		this.collector = collector;

		StatusListener listener = new StatusListener() {
			@Override
			public void onException(Exception arg0) {

			}

			@Override
			public void onTrackLimitationNotice(int arg0) {

			}

			@Override
			public void onStatus(Status tweet) {
				if(!tweet.isRetweet()) {
					if(tweet.getURLEntities().length == 0) {
						//logger.info("tweet: "+tweet);
						queue.offer(tweet);	
					}
				}
			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {

			}
		};

		TwitterStreamFactory factory = new TwitterStreamFactory();
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
		FilterQuery fq = new FilterQuery();
		String keywords[] = { "politics"};
		fq.track(keywords);
		String languages[] = new String[]{ "en"};
		fq.track(keywords);
	
		fq.language(languages);
		// Filter by region:
		// For example San Fransisko or New York
		// -122.75,36.8,-121.75,37.8,-74,40,-73,41
		/*double[][] loc = { { -122.75, 36.8 }, { -121.75, 37.8 }, { -74, 40 },
				{ -73, 41 } };
		fq.locations(loc);*/
		twitterStream.filter(fq);
		// twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status tweet = queue.poll();
		if (tweet == null) {
			Utils.sleep(100);
		} else {
			collector.emit(new Values(tweet));
		}

	}

	@Override
	public void close() {
		twitterStream.shutdown();
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
