package com.internetTracesTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class InternetTracesSplitBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Map toplogyConfiguration;

	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.outputCollector = collector;

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String packet = input.getStringByField("Packet");
		long spoutTime=input.getLongByField("SpoutTime");
		String[] array = packet.split(",");
		long splitBoltTime= System.currentTimeMillis();
		this.outputCollector.emit(new Values(array[8],spoutTime,splitBoltTime));
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Flow","SpoutTime","SplitBoltTime"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
