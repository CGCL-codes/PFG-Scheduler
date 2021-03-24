package com.zipFTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class zipFIstBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.outputCollector = collector;
    }

    public void execute(Tuple tuple) {
        String data = tuple.getString(0);
        long istBoltTime= System.currentTimeMillis();
        long spoutTime =tuple.getLong(1);
//System.out.println(data+" ===="+spoutTime);
//System.exit(0);
        this.outputCollector.emit(new Values(data,spoutTime,istBoltTime));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Data","readingDataTime","splitBoltTime"));
    }
}
