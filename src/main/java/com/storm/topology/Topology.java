package com.storm.topology;

import com.DChoices.DChoicesGrouping;
import com.ProposedTopologyBuilder.WebDocsBoltDistinctCount;
import com.ProposedTopologyBuilder.WebDocsBoltSplit;
import com.ProposedTopologyBuilder.WebDocsBoltsDataRecording;
import com.ProposedTopologyBuilder.WebDocsSpout;
import com.storm.proposedarchitecture.SwappingResult;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.Config;
//import org.apache.storm.StormSubmitter;
//import org.apache.storm.generated.AlreadyAliveException;
//import org.apache.storm.generated.AuthorizationException;
//import org.apache.storm.generated.InvalidTopologyException;
//import org.apache.storm.grouping.PartialKeyGrouping;
//import org.apache.storm.grouping.ShuffleGrouping;
//import org.apache.storm.topology.TopologyBuilder;
//import org.apache.storm.trident.partition.IndexHashGrouping;


public class Topology {
    // private static final Logger LOG = LoggerFactory.getLogger(PartialKeyGroupingMain.class);
    //InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException, IOException
    public static void main(String[] args) throws Exception {
     //   new SwappingResult().dataCalculation(args[0],args[1]);
        Config config = new Config();
        config.setNumWorkers(20);//20
////
        TopologyBuilder builder = new TopologyBuilder(); //imediate
        		builder.setSpout("read-ITF", new WebDocsSpout(), 4).setNumTasks(1);
		builder.setBolt("split-bolt", new WebDocsBoltSplit(),16).setNumTasks(50).shuffleGrouping("read-ITF");
		builder.setBolt("count-bolt", new WebDocsBoltDistinctCount(),20).
		customGrouping("split-bolt", new DChoicesGrouping()).
		setNumTasks(50);
		builder.setBolt("store", new WebDocsBoltsDataRecording(),6).setNumTasks(1).shuffleGrouping("count-bolt");


        //     builder.setSpout("read-zipfData", new zipFSpout(), 4).setNumTasks(1);
        //    builder.setBolt("zipfist-bolt", new zipFIstBolt(),).setNumTasks(50).
        //              shuffleGrouping("read-zipfData");
//        builder.setBolt("zipfseconf-bolt", new zipFCountBolt(),20).
//                customGrouping("zipfist-bolt", new ProposedPartitioningScheme()).setNumTasks(50);
//        builder.setBolt("last", new ZipFDataWrite(),4).
//                setNumTasks(1).shuffleGrouping("zipfseconf-bolt");
//        builder.setSpout("read-ITF", new InternetTracesSpout(), 4).setNumTasks(1);
//        builder.setBolt("split-bolt", new InternetTracesSplitBolt(),10).setNumTasks(50).
//                shuffleGrouping("read-ITF");
//        builder.setBolt("count-bolt", new InternetTracesFlowCountBolt(),20).
//                customGrouping("split-bolt", new PartialKeyGrouping()).setNumTasks(50);
//        builder.setSpout("read-webdoc", new WebDocsSpout(), 4).setNumTasks(1);
//        builder.setBolt("split-bolt", new WebDocsBoltSplit(),10).setNumTasks(50).
//                shuffleGrouping("read-webdoc");
//        builder.setBolt("count-bolt", new WebDocsBoltDistinctCount(),20).
//                customGrouping("split-bolt", new PartialKeyGrouping()).setNumTasks(50);

      //LocalCluster cluster = new LocalCluster();

        //StormSubmitter.submitTopology("LogAnalyserStorm", config,
         //builder.createTopology();

       //cluster.submitTopology("LoadCal", config, builder.createTopology());
     StormSubmitter.submitTopology("IT", config, builder.createTopology());
       //System.out.println("Submitt");
//        System.exit(0);
//Georgous music elevator

        System.out.println("Topology submitted");

    }
}
