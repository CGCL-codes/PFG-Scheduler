package com.stormConfiguration.LoadImbalanceForGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class ModuleGrouping implements CustomStreamGrouping {
	int numTasks = 0;
	AtomicLong Time;
	//HashMap<String, Model> map= new HashMap<String, Model>();

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		numTasks = targetTasks.size();
		Time= new AtomicLong(0);
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList();
		if (values.size() > 0) {
			String str = values.get(0).toString();
			
			if (str.isEmpty())
				boltIds.add(0);
			else
				//numTasks=str.charAt(0) % numTasks;
				boltIds.add(str.charAt(0) % numTasks);
		}
		Time= new AtomicLong(System.currentTimeMillis());
		//java.sql.Connection conn;
		//Statement statement = null;
		try {
			java.sql.Connection	conn = com.sql.databaseconnection.Connection.connetion();
			Statement statement=conn.createStatement();
			System.out.println(boltIds.get(0)+"BoltID IDEX");
			String query="insert into Annoymns (TaskID,Field,Time,Count) values ("
		    		+boltIds.get(0)+","+values.get(0).toString()+","+Time+","+
		    		1+")";
			statement.execute(query);
			statement.close();
			conn.close();
			System.out.println("added");
			//System.exit(0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return boltIds;
	}
}
