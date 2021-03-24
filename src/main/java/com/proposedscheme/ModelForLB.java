package com.proposedscheme;

public class ModelForLB {
	private String data;
	private String task;
	private String zipFlevel;
	public ModelForLB() {
		// TODO Auto-generated constructor stub
	}
	
	public ModelForLB(String data, String task, String zipFlevel) {
		
		this.data = data;
		this.task = task;
		this.zipFlevel = zipFlevel;
	}

	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getTask() {
		return task;
	}
	public void setTask(String task) {
		this.task = task;
	}
	public String getZipFlevel() {
		return zipFlevel;
	}
	public void setZipFlevel(String zipFlevel) {
		this.zipFlevel = zipFlevel;
	}

}
