package com.ireport.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.ireport.kafka.producer.parser.MessageParserUtil;

public class DummyProducer {
	private static final String SAMPLES_COMMENTS_TXT = "/Users/abatchu/Work/git/ireport_03122016/ireport/ireport-root/ireport-producer/resources/samples/comments.txt";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class",
				"com.ireport.kafka.producer.ClassificationPartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		try (BufferedReader br = new BufferedReader(new FileReader(
				SAMPLES_COMMENTS_TXT))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						"ireport", MessageParserUtil.parseMsg(line)
								.toJSONString());
				producer.send(data);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		producer.close();
	}
}
