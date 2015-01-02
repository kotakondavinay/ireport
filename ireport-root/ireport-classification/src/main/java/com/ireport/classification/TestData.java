package com.ireport.classification;

public class TestData {
	private String className;
	private double probValue;
	
	public TestData(String className,double probValue){
		this.className = className;
		this.probValue=probValue;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public double getProbValue() {
		return probValue;
	}

	public void setProbValue(double probValue) {
		this.probValue = probValue;
	}
}
