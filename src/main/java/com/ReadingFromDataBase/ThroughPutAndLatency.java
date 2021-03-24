package com.ReadingFromDataBase;

import java.io.*;
import java.util.HashMap;

public class ThroughPutAndLatency {
    public static void main(String[] args) throws Exception{
        int [] arrayBreakingpoint={10000000,20000000,30000000};
        for(int i=0;i<arrayBreakingpoint.length;i++) {
            new ThroughPutAndLatency().throughPutAndLatencyCalculation(args[0], args[1]+arrayBreakingpoint[i]+".csv", args[2]+arrayBreakingpoint[i]+".csv", arrayBreakingpoint[i]);
            System.out.println("Done" + arrayBreakingpoint[i]);
        }
    }

    public void throughPutAndLatencyCalculation(String inputAddress, String outPutAdressForThroughPut, String outputAddressForLatency, int breakingPoint) throws Exception{

        BufferedReader reader= new BufferedReader(new BufferedReader(new FileReader(new File(inputAddress))));
        HashMap<String, Integer> map= new HashMap();
        int count=0;
        long timeForEachTuple=0;
        String tuple=null;
        String row=null;
        BufferedWriter writerForLatency= new BufferedWriter(new FileWriter(new File(outputAddressForLatency)));
       // writerForLatency.write("Tuple"+","+"Time"+","+"\n");
        while((row= reader.readLine())!=null){
            count++;
            String [] array=row.split(",");
            if(array.length>=2) {
                int counter = map.getOrDefault(array[3], 0);
                counter++;
                map.put(array[3], counter);
                tuple = array[0];
                timeForEachTuple = Long.parseLong(array[3]) - Long.parseLong(array[1]);
            }
            writerForLatency.write(timeForEachTuple+"\n");
            if(count==breakingPoint){
                break;
            }
        }
        reader.close();
//        BufferedWriter writerForThroughPut= new BufferedWriter(new FileWriter(new File(outPutAdressForThroughPut)));
//        writerForThroughPut.write("Time,count,\n");
//        Iterator iterator=  map.entrySet().iterator();
//        while (iterator.hasNext()){
//            Map.Entry mapElement= (Map.Entry) iterator.next();
//            writerForThroughPut.write(mapElement.getKey()+","+mapElement.getValue()+"\n");
//
//        }
//        writerForThroughPut.flush();
//        writerForThroughPut.close();

        writerForLatency.flush();
        writerForLatency.close();

    }

}
