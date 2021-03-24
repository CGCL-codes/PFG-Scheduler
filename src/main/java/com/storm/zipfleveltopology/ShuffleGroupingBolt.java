package com.storm.zipfleveltopology;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class ShuffleGroupingBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
	//private KeyValueState<String, Long> flowCount;
	private OutputCollector collector;
	@Override
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(topoConf, context, collector);
		this.collector=collector;
	}
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		
		String line= input.getStringByField("line");
		
//		Long count= flowCount.get(line, (long) 0); /// Change Flow name Counter data[last Flow id ]
//		count++;
//		flowCount.put(line, count);/// Change Flow name Counter
		//System.out.println(data[0]+"  "+count);
		collector.ack(input);
	}

	public void initState(KeyValueState<String, Long> state) {
		// TODO Auto-generated method stub
		//this.collect
//		this.flowCount=state;
	}

}
