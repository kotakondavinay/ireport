package com.ireport.twitter.storm.topology.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSearch {

	public List<Status> search(String keyword, String authConsumerKey,
			String authConsumerSecret, String authAccessToken,
			String authTokenSecret) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(authConsumerKey);
		cb.setOAuthConsumerSecret(authConsumerSecret);
		cb.setOAuthAccessToken(authAccessToken);
		cb.setOAuthAccessTokenSecret(authTokenSecret);
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		Query query = new Query(
				keyword
						+ " -filter:retweets -filter:links -filter:replies -filter:images");
		query.setCount(100);
		query.setLocale("en");
		query.setLang("en");
		try {
			QueryResult queryResult = twitter.search(query);
			return queryResult.getTweets();
		} catch (TwitterException e) {
			// ignore
			e.printStackTrace();
		}
		return Collections.emptyList();
	}

	public static void main(String[] args) {
		TwitterSearch ts = new TwitterSearch();
		// String keyword =
		// "politics OR worst OR pathetic OR  OR resolve  OR poor service OR worst behaviour OR not good";
		String keyword = "politics OR worst OR not good OR pathetic  OR poor service";
		List<Status> search = ts.search(keyword, "", "", "", "");
		List<String> searchDesc = new ArrayList<String>();
		for (Status s : search) {
			searchDesc.add(" Geo: " + s.getGeoLocation() + " Tweet: "
					+ s.getText());
			System.out.println(" Geo: " + s.getGeoLocation() + " Tweet: "
					+ s.getText());
		}
	}

}