package com.ireport.kafka.producer.parser;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;

public class MessageParserUtil {
	private static final String MSG_PATTERN = "#(.*?)@(.*?)<(.*?)>";
	private static Pattern pattern;
	private static Matcher matcher;

	@SuppressWarnings({ "unchecked", "serial" })
	public static JSONObject parseMsg(String msg) {
		pattern = Pattern.compile(MSG_PATTERN);
		matcher = pattern.matcher(msg);
		JSONObject jsonObj = new JSONObject();
		if (matcher.find()) {
			jsonObj.putAll(new HashMap<String, String>() {
				{
					put("issue", matcher.group(1));
					put("occurance_spot", matcher.group(2));
					put("details", matcher.group(3));
					put("reporter_location", "Need to fetch from ip address");
				}
			});
			System.out.println(matcher.group(1) + " : " + matcher.group(2)
					+ " : " + matcher.group(3));
		}
		return jsonObj;
	}

	public static void main(String[] args) {
		JSONObject parseMsg = MessageParserUtil
				.parseMsg("#Worst service @xyz hotel, Bangalore <description1>");
		System.out.println(" JSON Object: " + parseMsg);
	}

}
