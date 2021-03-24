package com.ReadingFromDataBase;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HashFunctionCalculation {
    public void latency(String input, String output) throws Exception{
        BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
        int breakingPoint=0;
        String row=null;
        double averageLatency=0.0;
        double time=0.0;
        while((row=reader.readLine())!=null) {
            breakingPoint++;
            // System.out.println(row);
            String[] array = row.split(",");
           long latency= Long.parseLong(array[3])-Long.parseLong(array[1]);
           if(latency<0){
               latency=~latency;
           }
           time= latency;
           time= time/1000;
           averageLatency= averageLatency+time;
            if(breakingPoint==23599842){
                break;
            }
        }
        reader.close();
        writer.write(averageLatency+","+breakingPoint+"\n");
        writer.flush();
    }
public void loadOnOperators(String input, String output)throws  Exception{
    BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
    BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
    String row=null;
    HashMap<String, Long> map= new HashMap<String, Long>();
    int breakingPoint=0;
    while((row=reader.readLine())!=null){
       // System.out.println(row);
        String [] array= row.split(",");
        long count=map.getOrDefault(array[5],0L);
        count++;
        map.put(array[5],count);
        breakingPoint++;
        if(breakingPoint==23599842){
            break;
        }
    }
    reader.close();
    Iterator iterator=  map.entrySet().iterator();
    while (iterator.hasNext()){
        Map.Entry entry = (Map.Entry)iterator.next();
        writer.write(entry.getKey()+","+entry.getValue()+"\n");
    }
    writer.flush();
    writer.close();
}




}
