package com.ireport.twitter.storm.topology.utilities;

import java.util.ArrayList;
import java.util.List;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class PublicProfileTweet {
	public static void main(String[] args) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(args[0]);
		cb.setOAuthConsumerSecret(args[1]);
		cb.setOAuthAccessToken(args[2]);
		cb.setOAuthAccessTokenSecret(args[3]);
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		int pageno = 1;
		String user = "politicalbaaba";
		List<Status> statuses = new ArrayList<Status>();
		while (true) {
			try {
				int size = statuses.size();
				Paging page = new Paging(pageno++, 100);
				statuses.addAll(twitter.getUserTimeline(user, page));
				if (statuses.size() == size)
					break;
			} catch (TwitterException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Total: " + statuses.size());
		for (Status status : statuses) {
			System.out.println(" Tweet: " + status.getText());
		}
	}
}
