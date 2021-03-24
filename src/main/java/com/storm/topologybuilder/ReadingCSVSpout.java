package com.storm.topologybuilder;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ReadingCSVSpout extends BaseRichSpout {
	// private static final Logger LOG = LoggerFactory.getLogger(LineSpout.class);
	private String fileName;
	private SpoutOutputCollector _collector;
	private BufferedReader reader;
	private AtomicInteger linesRead;
	private AtomicLong Time;
	private BufferedWriter bufferedWriter;
	private int count;
	//private static final Logger LOG = LoggerFactory.getLogger(ReadingCSVSpout.class);
	private static Logger logger = Logger.getLogger("MyLog3");
	private static FileHandler fh;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		fileName = "/home/adeelaslam/Input/InternetTracesWFlowID.csv";
	//fileName="/public/home/abrar/Input/zipf_100000_10000_0.1.csv";
		count=0;
		try {
            fh = new FileHandler("/home/adeelaslam/Log/Mylog3.log");
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			reader = new BufferedReader(new FileReader(new File(fileName)));
			Time= new AtomicLong();
			linesRead = new AtomicInteger(0);
			_collector = collector;
		}
		catch (Exception e){
			//LOG.error(e.getMessage());
          logger.info("Line is read "+ e.getMessage() + " and emitted at time");
			throw new RuntimeException(e);
		}

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		super.deactivate();
		try {
			reader.close();
			bufferedWriter.close();

		} catch (Exception e) {
			// TODO: handle exception
			//System.out.println("Problem close");
		}
	}

	public void nextTuple() {
	count++;
			// TODO Auto-generated method stub

			try {

				String line = reader.readLine();
                //logger.info("Line is read.... "+ line + " and emitted at time");
				if (line != null) {
					Time= new AtomicLong(System.currentTimeMillis());
					long time= Time.longValue();
					//System.out.println(time);
					//System.exit(0);
					int count = linesRead.incrementAndGet();
					//	System.out.println();
					try {
						logger.info(""+count);
							if(count>2851734) {
								_collector.emit(new Values(line));
								logger.info("Line is read "+ line + " and emitted at time"+count);
							}

					//	logger.info("Line is read "+ line + " and emitted at time");
					}
					catch (IllegalArgumentException e) {
						// TODO: handle exception
						logger.info("Error:"+e.getMessage());
						//System.exit(0);
					}
				} else {
					//logger.info("Finished reading file, " + " lines read");
					Thread.sleep(10000);

				}
			} catch (Exception e) {
				e.printStackTrace();
			//	logger.info("Error:" + e);
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
		System.out.println("msgID" + msgId);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("Line"));

	}

}
