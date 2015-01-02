package com.ireport.classification;

import java.util.HashMap;
import java.util.Map;

public class TrainData {

	private String className;
	private Map<String,Integer> wordFreqMap;
	private Map<String,Double> probMap;
	private Map<String,Double> logMap;
	
	public TrainData(String className,Map<String,Integer> wordFreqMap, Map<String,Double> probMap, Map<String,Double> logMap){
		
		this.className = className;
		this.wordFreqMap = wordFreqMap;
		this.probMap = probMap;
		this.logMap = logMap;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public Map<String, Integer> getWordFreqMap() {
		return wordFreqMap;
	}

	public void setWordFreqMap(Map<String, Integer> wordFreqMap) {
		this.wordFreqMap = wordFreqMap;
	}

	public Map<String, Double> getProbMap() {
		return probMap;
	}

	public void setProbMap(Map<String, Double> probMap) {
		this.probMap = probMap;
	}

	public Map<String, Double> getLogMap() {
		return logMap;
	}

	public void setLogMap(Map<String, Double> logMap) {
		this.logMap = logMap;
	}
}
