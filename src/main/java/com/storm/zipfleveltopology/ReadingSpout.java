package com.storm.zipfleveltopology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class ReadingSpout implements IRichSpout {
	private String fileName;
	private SpoutOutputCollector outputCollector;
	BufferedReader reader;
	private int count;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
		try {
			count=0;
			//fileName="C:\\Users\\Adeel\\Desktop\\CodeZipFGenerator\\ZipfGenerator-master\\Output\\zipf_100000_10000_0.2.csv";
			fileName="/home/adeelaslam/InputForzipffiles/ZipFData.csv";
			this.outputCollector=collector;
			reader=  new BufferedReader(new FileReader(new File(fileName)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		count++;
		System.out.println("Count:--"+count);
		try {
			String line= reader.readLine();
			if (line != null) {
				//System.out.println("::Line--"+line);
			this.outputCollector.emit(new Values(line));
			}
			else {
				//System.out.println("Finished reading file, " + " lines read");
				Thread.sleep(10000);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("line"));
		
	}
	

	public void close() {
		// TODO Auto-generated method stub
		try {
			reader.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

}
