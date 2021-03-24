package com.internetTracesTopology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class InternetTracesSpout implements IRichSpout {
	private String fileName;
	private SpoutOutputCollector outputCollector;
	private BufferedReader reader;
	private int count;
	

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			fileName="/home/adeelaslam/Input/InternetTracesWFlowID.csv";
			//fileName = "G:\\adeel\\InternetTracesWFlowID.csv";
			this.outputCollector = collector;

			reader = new BufferedReader(new FileReader(new File(fileName)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void close() {
		// TODO Auto-generated method stub
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		try {


			String packet = reader.readLine();
			long currentTime= System.currentTimeMillis();
			if (packet != null) {
				
				this.outputCollector.emit(new Values(packet, currentTime));
				
			} else {
				Thread.sleep(10000);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Packet","SpoutTime"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
