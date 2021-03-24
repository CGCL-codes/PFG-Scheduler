package com.ProposedTopologyBuilder;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

public class WebDocsSpout extends BaseRichSpout {
	//private static final Logger LOG = LoggerFactory.getLogger(WebDocsSpout.class);
	private String fileName;
	private SpoutOutputCollector outputCollector;
	private BufferedReader bufferedReader;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub

		outputCollector=collector;
		try {
			fileName="/home/adeelaslam/InputForzipffiles/ZipFData.csv";
		 	bufferedReader= new BufferedReader(new FileReader(new File(fileName)));
		} catch (Exception e) {
			// TODO: handle exception
			throw new RuntimeException(e);
			//System.out.println("FileNotFoundExcception"+e);
		}
	}

	public void nextTuple() {
		//Utils.sleep(100);
		// TODO Auto-generated method stub
		try {
			String line= bufferedReader.readLine();
			if(line != null) {
				long currentTime= System.currentTimeMillis();
				outputCollector.emit(new Values(line,currentTime));
				//Thread.sleep(1);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Data","Time"));

	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		super.deactivate();
		try {
			bufferedReader.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("FileCompleted");
		}
	}
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}

}
