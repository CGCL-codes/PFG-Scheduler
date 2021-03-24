package com.storm.proposedarchitecture;

public class PreFilteringModel implements Comparable<PreFilteringModel>  {
	private  String key;
	private long newCount;
	private long oldCount;
	private long totalCount;
	
	public long getNewCount() {
		return newCount;
	}
	public void setNewCount(long newCount) {
		this.newCount = newCount;
	}
	public long getOldCount() {
		return oldCount;
	}
	public void setOldCount(long oldCount) {
		this.oldCount = oldCount;
	}
	public long getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(long totalCount) {
		this.totalCount = newCount-oldCount;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int compareTo(PreFilteringModel pre) {
		// TODO Auto-generated method stub
		if(this.getNewCount()> pre.getNewCount()) {
			return 1;
		}
		else if (this.getNewCount()<pre.getNewCount()) {
			return -1;
		}
		
		return 0;
	}
	
	
}
