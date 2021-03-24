package com.proposedscheme;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;


public class ReadingBolt extends BaseRichBolt {
	OutputCollector collector;

	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String line=input.getString(0);
	//	System.out.println(line+"--"+input.getInteger(1));
		this.collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
