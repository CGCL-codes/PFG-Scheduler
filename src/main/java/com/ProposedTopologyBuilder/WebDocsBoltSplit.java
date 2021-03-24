package com.ProposedTopologyBuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WebDocsBoltSplit extends BaseRichBolt {
	private OutputCollector outputCollector;
//	private static java.util.logging.Logger logger = java.util.logging.Logger.getLogger("MyLogSPOUT");
//	private static FileHandler fh;
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
		this.outputCollector = collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String data = input.getString(0);
//		String[] array=data.split(",");

		long splitSpoutTime= System.currentTimeMillis();
		long readingDataTime =input.getLong(1);
		//System.out.println(input);
		//if(array[1].equals("0.1"))
		outputCollector.emit(new Values(data,readingDataTime,splitSpoutTime));
	
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Data","readingDataTime","splitBoltTime"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
