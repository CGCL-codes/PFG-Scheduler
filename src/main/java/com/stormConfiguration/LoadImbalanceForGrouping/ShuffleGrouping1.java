package com.stormConfiguration.LoadImbalanceForGrouping;

import com.storm.models.Model;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class ShuffleGrouping1 implements CustomStreamGrouping, Serializable {
	private ArrayList<List<Integer>> choices;
	private AtomicInteger current;
	private AtomicInteger load;
	private ConcurrentHashMap<String, Model> loadMap;
	private AtomicLong time;
	private Connection	conn;
	private Statement statement;
	// private AtomicLongMap<String> loadMap;
	private static Logger logger = Logger.getLogger("MyLogSG");
	private static FileHandler fh;
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		choices = new ArrayList<List<Integer>>(targetTasks.size());
		// System.out.println("Size of Tasks "+targetTasks.size());

		// if(choices.size()==1)
		// System.exit(0);
		for (Integer i : targetTasks) {
			// System.out.println(i.highestOneBit(i));
			choices.add(Arrays.asList(i));

		}

		try {
			fh = new FileHandler("/home/adeelaslam/Log/SGDB.log");
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			conn = com.sql.databaseconnection.Connection.connetion();
			//System.out.println("connected data");
			statement=conn.createStatement();
			load = new AtomicInteger(0);
			loadMap = new ConcurrentHashMap<String, Model>();
			current = new AtomicInteger(0);
			time = new AtomicLong(0);
			Collections.shuffle(choices, new Random());

		}
		catch (Exception e){
			logger.info("Line is read "+ e.getMessage() + " and emitted at time");
		}
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// System.out.println(taskId);

		int rightNow;
		int loadFactor;
		String task;
		String data;
		int size = choices.size();
		
		while (true) {
			loadFactor=load.incrementAndGet();
			rightNow = current.incrementAndGet();
			//System.out.println(rightNow + " value for other " + values.get(0));
			//System.out.println();
			// System.out.println("Task Id----------------"+taskId);
			//System.exit(0);
			//data=values.get(0);
			
			///// IT should be monitiored and controlled from here put here the size of map to the number of tasks
			//mentioned in topology building
			if(loadMap.size()>=50) {
				//System.out.println(loadMap.size());
				//System.exit(0);
				try {
					ArrayList<String> list= new ArrayList<String>();
				//	Connection conn=com.sql.databaseconnection.Connection.connetion();
				//	Statement statement=conn.createStatement();
					Enumeration<String> keys = loadMap.keys();
					while ( keys.hasMoreElements()) {
						
					    String key  = keys.nextElement();
					    Model loadvalue = loadMap.get(key);
					    String arr=loadvalue.getKey();
					    String[] array=arr.split(",");

					    //System.out.println("Key"+key+"value"+ loadvalue);
					    String query="insert into sg_syn (Tasks,Data,Count,Time,zipflevel) values ('"
					    		+key+"','"+loadvalue.getKey()+"',"+loadvalue.getLoadCount()+","+
					    		loadvalue.getTime()+","+array[1]+")";
					   list.add(query);
					   
					}
					//System.out.println(list.size()+"Size");
					//System.exit(0);
					loadMap= new ConcurrentHashMap<String, Model>();
					
					//statement.execute("TRUNCATE TABLE load_calculation.shufflegrouping");
					for(String querrry:list) {
						statement.addBatch(querrry);
					}
					//statement.executeBatch();
					//loadFactor=0;
					 statement.executeBatch();
					//logger.info("Line is read succ " + " and emitted at time");
					//System.exit(0);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logger.info("Line is read "+ e.getMessage() + " and emitted at time");
					close();
					e.printStackTrace();
				}
			}
			if (rightNow < size) {
				data = values.get(0).toString();
				task = choices.get(rightNow).get(0).toString() ;
				
				time= new AtomicLong(System.currentTimeMillis());
				Model model= new Model(data, (long)1, time);
				if (loadMap != null) {
					if (loadMap.contains(task)) {
						model= loadMap.get(task);
						model= new Model(data, model.getLoadCount()+1, time);
						loadMap.replace(task, model);
						System.out.println("D");
						///System.exit(0);
					} else {
						loadMap.put(task, model);
					}
				}

				return choices.get(rightNow);

			} else if (rightNow == size) {

				current.set(0);
				data=values.get(0).toString();
				time= new AtomicLong(System.currentTimeMillis());
				Model model= new Model(data, (long)1, time);
				task = choices.get(0).get(0).toString();
				if (loadMap != null) {
					if (loadMap.contains(task)) {
						model= loadMap.get(task);
						model= new Model(data, model.getLoadCount()+1, time);
						
							} else {
						loadMap.put(task, model);
					}
				}

				return choices.get(0);
			}
		} // race condition with another thread, and we lost. try again
	}
	public void close(){
		try{
			statement.close();
			conn.close();
		}catch (Exception e){

		}
	}

}
