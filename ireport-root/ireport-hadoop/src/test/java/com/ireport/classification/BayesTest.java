package com.ireport.classification;

import org.junit.Test;

public class BayesTest {

	BayesLearner bayesLearner = new BayesLearner(); 
	String trainDir = "/opt/learning/train";
	String stopWordsDir = "/opt/learning/stopwords";
	
	@Test
	public void testBayes() {
		bayesLearner.setStopWords(stopWordsDir);
		bayesLearner.trainData(trainDir);
		String classifier = bayesLearner.classifyData("i love computers");
		System.out.println("classifier is "+classifier);
	}
	
}
