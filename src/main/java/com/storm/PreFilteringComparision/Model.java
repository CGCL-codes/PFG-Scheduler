package com.storm.PreFilteringComparision;

public class Model {
	private String dataitem;
	private int predictedCount;
	private int actualCount;
	public String getDataitem() {
		return dataitem;
	}
	public void setDataitem(String dataitem) {
		this.dataitem = dataitem;
	}
	public int getPredictedCount() {
		return predictedCount;
	}
	public void setPredictedCount(int predictedCount) {
		this.predictedCount = predictedCount;
	}
	public int getActualCount() {
		return actualCount;
	}
	public void setActualCount(int actualCount) {
		this.actualCount = actualCount;
	}
	@Override
	public String toString() {
		return "Model [dataitem=" + dataitem + ", predictedCount=" + predictedCount + ", actualCount=" + actualCount
				+ "]";
	}
	
}
