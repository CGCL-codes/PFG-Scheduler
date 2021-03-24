package com.stormConfiguration.LoadImbalanceForGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.com.google.common.hash.HashFunction;
import org.apache.storm.shade.com.google.common.hash.Hashing;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PartialKeyGrouping implements CustomStreamGrouping{
	AtomicLong Time;
	private List<Integer> targetTasks;
    private long[] targetTaskStats;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
//	private static java.util.logging.Logger logger = Logger.getLogger("MyLogPKG");
//	private static FileHandler fh;
//	private java.sql.Connection	conn;
//	private Statement statement;
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
	 
		//System.out.println(context.getSources("call-log-reader-spout")+ "componentids");
		//System.exit(0);
		//Time= new AtomicLong(0);
		this.targetTasks = targetTasks;
//        targetTaskStats = new long[this.targetTasks.size()];
//        try{
//			fh = new FileHandler("/home/adeelaslam/Log/PKG");
//			logger.addHandler(fh);
//			SimpleFormatter formatter = new SimpleFormatter();
//			fh.setFormatter(formatter);
//				conn = com.sql.databaseconnection.Connection.connetion();
//			System.out.println("connected data");
//			 statement=conn.createStatement();
//		}
//        catch (Exception e){
//
//		}
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		//System.out.println(values.size());
		//System.exit(0);
		List<Integer> boltIds = new ArrayList<Integer>(1);
        if (values.size() > 0) {
            String str = values.get(0).toString(); // assume key is the first field
           
            int firstChoice = (int) (Math.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
            int secondChoice = (int) (Math.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
            boltIds.add(targetTasks.get(selected));
            targetTaskStats[selected]++;
           // Time= new AtomicLong(System.currentTimeMillis());
//            try {
////    			java.sql.Connection	conn = com.sql.databaseconnection.Connection.connetion();
////    			System.out.println("connected data");
////    			Statement statement=conn.createStatement();
//    			//System.out.println(boltIds.get(0)+"BoltID IDEX");
//				String arr=values.get(0).toString();
//				String [] array= arr.split(",");
//    			String query="insert into pkg_syn (Taskid,Field,Time,LoadCount,zipflevel) values ('"
//    		    		+boltIds.get(0)+"','"+values.get(0).toString()+"',"+Time+","+
//    		    		1+","+array[1]+")";
//    			//System.out.println(query);
//    			statement.execute(query);
//    			//logger.info("Inserted:---"+query);
//
//    			//System.out.println("added");
//
//    		} catch (Exception e) {
//    			// TODO Auto-generated catch block
//				logger.info("Exception:---"+e.getMessage());
//
//					close();
//
//				e.printStackTrace();
//    			//System.exit(0);
//    		}
            
        }


		return boltIds;
	}
	public void close(){

	}
}
