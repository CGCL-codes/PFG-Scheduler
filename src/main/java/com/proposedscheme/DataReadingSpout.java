package com.proposedscheme;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataReadingSpout extends BaseRichSpout {
	private String fileName;
	private SpoutOutputCollector collector;
	private BufferedReader reader;
	private AtomicInteger linesRead;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		linesRead = new AtomicInteger(0);
		try {
			fileName="/home/adeelaslam/InputForzipffiles/ZipFData.csv";
			reader = new BufferedReader(new FileReader(new File(fileName)));
		} catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException();
		}
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			String line = reader.readLine();
			//String [] array= line.split(",");
			int count=linesRead.getAndIncrement();
			if ((line != null)) {
				//System.out.println(line);
				
				collector.emit(new Values(line));
			} 

			else  {
				reader.close();
				//System.out.println("Finished reading file, " + " lines read--"+count);
				//Thread.sleep(10000);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("ZipF"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
