package com.zipFTopology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

public class zipFSpout extends BaseRichSpout {
    private String fileName;
    private SpoutOutputCollector outputCollector;
    private BufferedReader bufferedReader;
    int count;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
       this.outputCollector=collector;
       this.count=0;
        try {
          fileName="E:\\WSpace2019\\StormLoad_balancing\\Output\\zipf_1000_100_0.1.csv";
           // fileName="/home/adeelaslam/ZipFData/zipf_32000000_8000000_0.5.csv";
//			fileName="G:\\WorkSpaceForPHDBigData\\FrequentDataSetMining\\Output\\Total"
//					+ "CountViaDistinctElement.csv";
            this.outputCollector=collector;
            bufferedReader= new BufferedReader(new FileReader(new File(fileName)));
        } catch (Exception e) {
            // TODO: handle exception
            throw new RuntimeException(e);
            //System.out.println("FileNotFoundExcception"+e);
        }
    }

    public void nextTuple() {
        try {
            count++;
          // System.out.println(count+"---");
            String line= bufferedReader.readLine();

            if(line != null) {
               // System.out.println(line+"---"+count);
                long currentTime= System.currentTimeMillis();
                this.outputCollector.emit(new Values(line,currentTime));

            }
            if(line.equals(null)){
                System.out.println(line+"---"+count);
              //  bufferedReader.close();
            }
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    @Override
    public void close() {
        super.close();
        try{
            bufferedReader.close();
        }
        catch (Exception e){

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Data","Time"));
    }
}
