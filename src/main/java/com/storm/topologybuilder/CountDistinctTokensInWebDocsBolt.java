package com.storm.topologybuilder;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CountDistinctTokensInWebDocsBolt extends BaseStatefulBolt<KeyValueState<String, Long>>{
private KeyValueState<String, Long> wordcount;
private OutputCollector collector;
	//private static java.util.logging.Logger logger = Logger.getLogger("MyLog");
	//private static FileHandler fh;
@Override
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(topoConf, context, collector);
		try {
			this.collector = collector;

			//fh = new FileHandler("/public/home/abrar/loger/Mylog2.log");
			//logger.addHandler(fh);
			//SimpleFormatter formatter = new SimpleFormatter();
			//fh.setFormatter(formatter);
		}
		catch (Exception e){

		}

	}
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String line= input.getStringByField("Line");
		//int C= input.getInteger(1);
		//logger.info("Line is read bolt "+ line + " and emitted at time");
		String [] data= line.split(",");
		Long count= wordcount.get(data[8], (long) 0);//data[8]
		count++;
		wordcount.put(data[8], count);//data[8]
		//System.out.println(data[0]+"  "+count);
		collector.ack(input);
	}

	public void initState(KeyValueState<String, Long> state) {
		// TODO Auto-generated method stub
		this.wordcount=state;
		
	}

}
