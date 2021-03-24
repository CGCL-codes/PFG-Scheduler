package com.stormConfiguration.LoadImbalanceForGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;

public class KeyGrouping implements CustomStreamGrouping {
	int index;
    List<Integer> targets;
   // AtomicLong Time;
	//private java.sql.Connection	conn;
	//private Statement statement;
	//private static java.util.logging.Logger logger = Logger.getLogger("MyLogKG");
	//private static FileHandler fh;
	public static int objectToIndex(Object val, int numPartitions) {
        if (val == null) {
            return 0;
        }
        return Utils.toPositive(val.hashCode()) % numPartitions;
    }

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		try{
//			fh = new FileHandler("/home/adeelaslam/Log/KGDB.log");
//			logger.addHandler(fh);
//			SimpleFormatter formatter = new SimpleFormatter();
//			fh.setFormatter(formatter);
//			conn = com.sql.databaseconnection.Connection.connetion();
//			//System.out.println("connected data");
//			statement=conn.createStatement();
		targets = targetTasks;
		//Time= new AtomicLong(0);
	}
		catch(Exception e){
//logger.info(e.getMessage()+" KG---");
		}
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		int i = objectToIndex(values.get(index), targets.size());
		//Time= new AtomicLong(System.currentTimeMillis());
//		try {
////			java.sql.Connection	conn = com.sql.databaseconnection.Connection.connetion();
////			Statement statement=conn.createStatement();
//		//	System.out.println(boltIds.get(0)+"BoltID IDEX");
////			String arr= values.get(0).toString();
////			String [] array=arr.split(",");
////			String query="insert into kg_syn (TaskID,Field,Time,Count,zipflevel) values ('"
////		    		+targets.get(i)+"','"+values.get(0).toString()+"',"+Time+","+
////		    		1+","+array[1]+")";
////			statement.execute(query);
//		//	statement.close();
//		//	conn.close();
//			//System.out.println("added");
//		//	logger.info(query);
//			//System.exit(0);
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//		//logger.info(e.getMessage());
//			close();
//			//e.printStackTrace();
//		}
        return Arrays.asList(targets.get(i));
	}
	public void close(){
//		try{
//			statement.close();
//			conn.close();
//		}catch (Exception e){
//
//		}
	}
}
