package com.ProposedTopologyBuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class WebDocsBoltsDataRecording extends BaseRichBolt {
	private BufferedWriter writer;
	//private AtomicInteger count;
	private OutputCollector outputCollector;
	private int countforOverallTuples;
	private int countForFileWriting;
	private int counter=0;
	private StringBuilder sb;
	private static Logger logger = Logger.getLogger("ZipFLog");
	private static FileHandler fh;
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.outputCollector = collector;
		try {
			sb = new StringBuilder();
			countForFileWriting = 0;
		//	countforOverallTuples = 0;
		//	counter=0;
			//count = new AtomicInteger(0);
			writer = new BufferedWriter(new FileWriter(new File("/home/adeelaslam/LoadForMemoryOverhead/ZipFData.csv")));
			fh = new FileHandler("/home/adeelaslam/Log/DataRecording");
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub

		String data = input.getString(0);
		Long countTime = input.getLong(1);
		String[] array = data.split(",");

		sb.append(array[0]+","+array[1]+","+array[2]+","+countTime+ ","+array[3]+","+array[4]+","+array[5]+","+array[6]+","+
				input.getSourceTask()+"\n");
		//countforOverallTuples = count.getAndIncrement();
		countForFileWriting++;
		if((countForFileWriting==100000)) {
			try {
				counter++;
				//logger.info("Inserted:---"+counter+ "---"+countForFileWriting);
				writer.write(sb.toString());
				countForFileWriting=0;
				logger.info("Inserted:---"+counter+ "---"+countForFileWriting);
				sb= new StringBuilder();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.info("Exception:---"+e+ "---");
			}
		}

		this.outputCollector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
