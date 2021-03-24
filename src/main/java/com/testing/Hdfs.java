package com.testing;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Hdfs{
    public static void main(String[] args) throws  Exception {
new Hdfs().fileReadAndWrite(args[0],args[1]);
    }

    public void fileReadAndWrite(String input, String outPut) throws  Exception{
        BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
        BufferedWriter writer= new BufferedWriter( new FileWriter(new File(outPut)));
        HashMap<String, Integer> map= new HashMap<String, Integer>();
        String row=null;
        int counter=0;
        int counter1=0;
        while((row=reader.readLine())!=null){
            String [] array= row.split(",");

                int count = map.getOrDefault(array[4], 0);
                count++;
                map.put(array[4], count);
            counter++;
            if(counter==40000000){
                break;
            }

        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
           writer.write(entry.getKey()+","+entry.getValue()+"\n");
        }
        writer.flush();
        writer.close();
        reader.close();
    }

}
