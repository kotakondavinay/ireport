package com.ireport.kafka.producer.parser;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;

import com.ireport.kafka.producer.Constants;

public class MessageParserUtil implements Constants {
	private static Pattern pattern;
	private static Matcher matcher;

	@SuppressWarnings({ "unchecked", "serial" })
	public static JSONObject parseMsg(final String msg) {
		pattern = Pattern.compile(MSG_PATTERN);
		matcher = pattern.matcher(msg);
		JSONObject jsonObj = new JSONObject();
		if (matcher.find()) {
			jsonObj.putAll(new HashMap<String, String>() {
				{
					put(ISSUE, matcher.group(1));
					put(OCCURANCE_SPOT, matcher.group(2));
					put(DETAILS, matcher.group(3));
					put(REPORTER_LOCATION, "Need to fetch from ip address");
					if (matcher.group(4) == null || matcher.group(4).equals("")) {
						put(PRIORITY_TYPE, "NORMAL");
					} else {
						put(PRIORITY_TYPE, matcher.group(4));
					}
					put(RAW_MSG, msg);
				}
			});
		}
		return jsonObj;
	}

}
