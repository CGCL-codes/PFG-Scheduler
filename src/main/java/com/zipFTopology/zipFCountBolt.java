package com.zipFTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class zipFCountBolt extends BaseRichBolt {
    private ConcurrentHashMap<String, Long> countingDistinctCount;
    private OutputCollector outputCollector;
    //private java.sql.Connection connection;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        try {
           //dataList= new ArrayList<String>();
         //   connection = Connection.connetion();
            this.outputCollector = collector;
            countingDistinctCount = new ConcurrentHashMap<String, Long>();
        } catch (Exception e) {
            // TODO: handle exception

        }
    }

    public void execute(Tuple tuple) {
        long istBoltTime = tuple.getLongByField("splitBoltTime");
        String data = tuple.getStringByField("Data");
        long spoutTime = tuple.getLongByField("readingDataTime");
        Long count = countingDistinctCount.getOrDefault(data, (long) 0);
        count++;
        countingDistinctCount.put(data, count);
//System.out.println(istBoltTime+"==="+data+"=="+count);
        long countBoltTime = System.currentTimeMillis();
        Statement statement;
        double zipfLevel=0.1;
//        try {
//            statement = connection.createStatement();
//            String query = "insert into zipf_proposed_scheme_5(data,spout_time,\r\n" +
//                    "bolt_time,bolt_count_time,zipflevel) values ( '"+data+"', " + spoutTime+
//                    ", "+istBoltTime+" , "+countBoltTime+" ,"+zipfLevel+")";
//            //    System.out.println(query);
//            statement.execute(query);
//            //System.out.println("Entered");
//            //System.exit(0);
//            statement.close();
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
////			System.out.println(e);
////			System.exit(0);
//            try {
//                connection.close();
//            } catch (SQLException e1) {
//                // TODO Auto-generated catch block
//                //e1.printStackTrace();
//            }
//            //e.printStackTrace();
//        }

        // outputCollector.emit(new Values(data, readingDataTime, splitBoltTime,
        // countBoltTime));
        this.outputCollector.emit(new Values(data,spoutTime,istBoltTime,countBoltTime));
       // outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data","spoutTime","istBoltTime","countBoltTime"));
    }
}
