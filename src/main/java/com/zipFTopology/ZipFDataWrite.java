package com.zipFTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

public class ZipFDataWrite extends BaseRichBolt {
    BufferedWriter writer;
    OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        String address="E:\\WSpace2019\\StormLoad_balancing\\Output\\result2.csv";
        this.collector=outputCollector;
        try {
            writer= new BufferedWriter(new FileWriter(new File(address)));
        }
        catch (Exception io)
        {
            try{
            //    writer.close();
            }
            catch (Exception i){

            }
        }
    }

    public void execute(Tuple tuple) {
        try {

            if(tuple!=null) {
                //   System.out.println("Here it is");
                writer.write(tuple.getStringByField("data") + "," + tuple.getLongByField("spoutTime") + "," +
                        tuple.getLongByField("istBoltTime") + "," + tuple.getLongByField("countBoltTime") + "\n");
            }


        }
        catch (Exception io){{
            try {
               // writer.close();
            }catch (Exception E){


            }
        }}
    this.collector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
