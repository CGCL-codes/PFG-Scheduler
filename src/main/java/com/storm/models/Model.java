package com.storm.models;

import java.util.concurrent.atomic.AtomicLong;

public class Model {
private String data;
private Long loadCount;
private AtomicLong time;

public Model(String data, Long loadCount, AtomicLong time) {
	//super();
	this.data = data;
	this.loadCount = loadCount;
	this.time = time;
}
public String getKey() {
	return data;
}
public void setKey(String key) {
	this.data = key;
}
public Long getLoadCount() {
	return loadCount;
}
public void setLoadCount(Long loadCount) {
	this.loadCount = loadCount;
}
public AtomicLong getTime() {
	return time;
}
public void setTime(AtomicLong time) {
	this.time = time;
}
}
