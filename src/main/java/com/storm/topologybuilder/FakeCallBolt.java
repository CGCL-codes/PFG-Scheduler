package com.storm.topologybuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class FakeCallBolt implements IRichBolt  {
	private OutputCollector collector;
	
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		 this.collector = collector;
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String from = tuple.getString(0);
		System.out.println("from---"+from);
	     int to = tuple.getInteger(1);
	     System.out.println("to---"+to);
	     System.exit(0);
	    // long f=tuple.getLong(2);
	      //System.out.println("Bolt1-------------"+from);
	  //System.exit(0);
	      //Integer duration = tuple.getInteger(2);
	      collector.ack(tuple);
	    //  collector.e
	      //System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
	     // System.out.println(tuple.getInteger(2));
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("call", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
