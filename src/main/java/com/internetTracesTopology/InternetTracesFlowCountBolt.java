package com.internetTracesTopology;

import com.sql.databaseconnection.Connection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InternetTracesFlowCountBolt extends BaseRichBolt {
	private ConcurrentHashMap<String, Long> countingFlows;
	private OutputCollector outputCollector;
	private java.sql.Connection connection;
	public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			connection = Connection.connetion();
			this.outputCollector = collector;
			countingFlows = new ConcurrentHashMap<String, Long>();
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String flow= input.getStringByField("Flow");
		long spoutTime= input.getLongByField("SpoutTime");
		long splitBoltTime=input.getLongByField("SplitBoltTime");
		Long count = countingFlows.getOrDefault(flow, (long) 0);
		count++;
		countingFlows.put(flow, count);

		long countBoltTime = System.currentTimeMillis();
		Statement statement;
		try {
			statement = connection.createStatement();
			String query = "insert into proposed_scheme_internet_traces (flow,flow_spout_time,\r\n" + 
					"flow_split_time,flow_count_time,data_set,scheme) values ( '"+flow+"', " + spoutTime+
					", "+splitBoltTime+" , "+countBoltTime+" , 'Internet_traces','PKG')";
			
			statement.execute(query);
			System.out.println("Entered");
			//System.exit(0);
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
//			System.out.println(e);
//			System.exit(0);
			try {
				connection.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				//e1.printStackTrace();
			}
			//e.printStackTrace();
		}

		// outputCollector.emit(new Values(data, readingDataTime, splitBoltTime,
		// countBoltTime));
		outputCollector.ack(input);
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
