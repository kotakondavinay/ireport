package com.ireport.classification;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BayesLearner {

	private List<TrainData> trainingList = new ArrayList<TrainData>();
	private List<String> stopWords = new ArrayList<String>();

	public void setStopWords(String stopWordsDir) {
		// Stop words.
		// training Data.
		File dir = new File(stopWordsDir);
		File[] files = dir.listFiles();
		// List<String> stopWords = new ArrayList<String>();
		for (File f : files) {

			if (f.isFile()) {
				// String name = f.toString();
				BufferedReader bufferedReader = null;
				try {
					bufferedReader = new BufferedReader(new FileReader(f));
					String line;
					// int count = 0;
					while ((line = bufferedReader.readLine()) != null) {

						String words[] = line.split("\\W+");

						for (String word : words) {
							if (!stopWords.contains(word)) {
								stopWords.add(word);
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
					// return stopWords;
				}
			}
		}
		// return stopWords;
	}

	public void trainData(String trainDir) {

		// training Data.
		File dir = new File(trainDir);
		File[] files = dir.listFiles();

		for (File f : files) {

			if (f.isFile()) {
				Map<String, Integer> map = new HashMap<String, Integer>();
				Map<String, Double> probMap = new HashMap<String, Double>();
				Map<String, Double> logMap = new HashMap<String, Double>();

				String name = f.toString();
				TrainData data = new TrainData(name, map, probMap, logMap);
				trainingList.add(data);
				BufferedReader bufferedReader = null;
				try {

					bufferedReader = new BufferedReader(new FileReader(f));
					String line;
					int count = 0;
					while ((line = bufferedReader.readLine()) != null) {
						String words[] = line.split("\\W+");

						for (String word : words) {
							count++;

							if (!stopWords.contains(word)) {
								if (map.containsKey(word)) {
									int value = map.get(word).intValue();
									map.put(word, value + 1);
								} else {
									map.put(word, 1);
								}
							}
						}
					}

					// System.out.println("Count : "+ count);
					// print all keys
					// System.out.println("printing data for "+f);
					Set<String> keys = map.keySet();

					for (String key : keys) {

						int value = map.get(key);
						double normalize = ((double) value) / count;
						double logValue = Math.log(normalize);
						probMap.put(key, normalize);
						logMap.put(key, logValue);
						// System.out.println("[ "+key+", "+ map.get(key)+", "+
						// probMap.get(key)+", "+ logMap.get(key)+"]");

					}

				} catch (IOException e) {
					e.printStackTrace();
				}

			} // end if
		}
	}

	public String classifyData(String line) {

		// ArrayList<TestData> probList = new ArrayList<TestData>();
		HashMap<String, Double> probList = new HashMap<String, Double>();

		// FileReader reader = new
		// FileReader("/home/dmalladi/workspace-git/SampleClassification/src/Computers.txt");

		for (int i = 0; i < trainingList.size(); i++) {

			// TestData data = new
			// TestData(trainingList.get(i).getClassName(),-13);
			// probList.add(data);

			probList.put(trainingList.get(i).getClassName(), -12.0);
		}
		String words[] = line.split("\\W+");

		for (String word : words) {
			if(!stopWords.contains(word)) {
				for (int i = 0; i < trainingList.size(); i++) {
					if (trainingList.get(i).getLogMap().containsKey(word)) {
						double value = probList.get(trainingList.get(i).getClassName())
								.doubleValue()
								+ trainingList.get(i).getLogMap().get(word);

						probList.put(trainingList.get(i).getClassName(), value);
					} else {
						probList.put(trainingList.get(i).getClassName(), -12.0+ probList.get(trainingList.get(i).getClassName()).doubleValue() );
					}
				}	
			} else {
				System.out.println("skipping stop word "+word);
			}
			
		}

		double maxProb = probList.get(trainingList.get(0).getClassName());
		double prob;
		String className = trainingList.get(0).getClassName();
		for (int i = 0; i < trainingList.size(); i++) {
			prob = probList.get(trainingList.get(i).getClassName());
			System.out.println("the probability for is: " + prob
					+ "the class name is: "
					+ trainingList.get(i).getClassName());
			if (prob > maxProb) {
				maxProb = prob;
				className = trainingList.get(i).getClassName();
			}
		}
		System.out.println("the probability is: " + maxProb
				+ "the class name is: " + className);
		return className;
	}
}