package com.ReadingFromDataBase;



import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MachineLoad {

    public void dataLoad(String input, String output) throws Exception{
        int counter=0;
        BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
        HashMap<String, Integer> map= new HashMap<String, Integer>();
        String row= null;
        while((row=reader.readLine())!=null){
            counter++;
            String [] array= row.split(",");
            int count= map.getOrDefault(array[4],0);
            count++;
            map.put(array[4],count);
          if(counter==28000000){
              break;
          }
        }
        Iterator iterator= map.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry entry = (Map.Entry)iterator.next();
            writer.write(entry.getKey()+","+entry.getValue()+"\n");
        }
        writer.flush();
        writer.close();
    }
}
