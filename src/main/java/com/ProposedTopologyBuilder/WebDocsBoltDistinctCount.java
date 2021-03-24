package com.ProposedTopologyBuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class WebDocsBoltDistinctCount extends BaseRichBolt{

	private ConcurrentHashMap<String, Long> countingFlows;
	private OutputCollector outputCollector;
	private StringBuilder sb;
	private static Logger logger = Logger.getLogger("ZipFLog");
	private static FileHandler fh;
	private int countForFileWriting;
	private int counter;
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub

		//connection = Connection.connetion();


			this.outputCollector = collector;
		countingFlows = new ConcurrentHashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		//System.out.println("Tuple"+tuple);
		// TODO Auto-generated method stub
		String data = tuple.getStringByField("Data");
//		Long count = countingFlows.getOrDefault(data, (long) 0);
//		count++;
//		countingFlows.put(data, count);
		long countBoltTime = System.currentTimeMillis();
		long splitBoltTime = tuple.getLongByField("splitBoltTime");
		long readingDataTime = tuple.getLongByField("readingDataTime");
		InetAddress ip = null;
		String hostname = null;
		try {
			ip = InetAddress.getLocalHost();
			hostname = ip.getHostName();
		} catch (Exception e) {

		}
		String tuples = data + "," + readingDataTime + "," + splitBoltTime + "," + hostname + "," + tuple.getSourceTask();
//System.out.println(data+"-"+readingDataTime+"-"+splitBoltTime+"-"+countBoltTime);
		// e.printStackTrace();
//		countForFileWriting++;
//		if ((countForFileWriting == 100000)) {
//			try {
//				counter++;
//
//				countForFileWriting = 0;
//				logger.info("Inserted:---" + counter + "---" + countForFileWriting);
//				sb = new StringBuilder();
//
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				logger.info("Exception:---" + e + "---");
//			}

			outputCollector.emit(new Values(tuples, countBoltTime));
			//outputCollector.ack(tuple);
			//System.out.println("Data");
		}

//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		// TODO Auto-generated method stub
//
//		declarer.declare(new Fields("tuple", "countBoltTime"));
//	}}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple", "countBoltTime"));
	}
}
