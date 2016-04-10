package com.ireport.kafka.producer.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class WordCountTest {
	private static final String TMP_WORDCOUNT_TXT = "/tmp/wordcount.txt";
	private static Random _rand = new Random();

	public static void main(String[] args) {
		String[] sentences = new String[] { "the cow jumped over the moon",
				"an apple a day keeps the doctor away",
				"four score and seven years ago",
				"snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = null;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
				TMP_WORDCOUNT_TXT)))) {
			while (true) {
				sentence = sentences[_rand.nextInt(sentences.length)];
				bw.write(sentence);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
