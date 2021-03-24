package com.storm.PreFilteringComparision;

import com.ReadingFromDataBase.LatencyCalculation;
import com.ReadingFromDataBase.ThroughPutAndLatency;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class AverageRelativeError {
    public static void main(String[] args) throws  Exception {
new LatencyCalculation().latencyCalculation(args[0],args[1]);
        //new ThroughPutAndLatency().throughPutAndLatencyCalculation(args[0],args[1],args[2],Integer.parseInt(args[3]));
//        BufferedReader reader= new BufferedReader(new FileReader(new File(args[0])));
//        String row=null;
//        int count=0;
//        int counter=0;
//        while((row=reader.readLine())!=null){
//            counter++;
//            String [] array= row.split(",");
//            count=count+Integer.parseInt(array[1]);
//           // System.out.println(row);
//        }
//        System.out.println(count/counter);
    }
//    public static void main(String[] args) throws Exception{
//        HashMap<String, Integer> map = new AverageRelativeError().readData("G:\\adeel\\1.0.csv");
//        System.out.println(map.size());
//        Iterator it = map.entrySet().iterator();
//        double dataAverage = 0;
//        while (it.hasNext()) {
//            Map.Entry pair = (Map.Entry) it.next();
//
//            dataAverage = dataAverage + (int) pair.getValue();
//        }
//
//        dataAverage = dataAverage / 50;
//
//        double max = Collections.max(map.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getValue();
//        // writer.write(array[i]+"," + roundTwoDecimals(max - dataAverage) + "\n");
//        System.out.println("f"+"----"+roundTwoDecimals(max - dataAverage));
//    }
//    public void dataCalculation(String input, double ZipF) throws  Exception{
//
////        BufferedWriter writer = new BufferedWriter(
////                new FileWriter(new File(output)));
////        int[] array={750000,1500000,2250000,3000000,3750000,4500000,5250000,6000000,6750000,7500000};
////        for(int i=0;i<array.length;i++) {
//            HashMap<String, Integer> map = new AverageRelativeError().readData(input);
//            System.out.println(map.size());
//            Iterator it = map.entrySet().iterator();
//            double dataAverage = 0;
//            while (it.hasNext()) {
//                Map.Entry pair = (Map.Entry) it.next();
//
//                dataAverage = dataAverage + (int) pair.getValue();
//            }
//
//            dataAverage = dataAverage / 50;
//
//            double max = Collections.max(map.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getValue();
//           // writer.write(array[i]+"," + roundTwoDecimals(max - dataAverage) + "\n");
//            System.out.println("f"+"----"+roundTwoDecimals(max - dataAverage));
//        }
        //.flush();
       // writer.close();
    //}
    static double roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#.##");
        return Double.valueOf(twoDForm.format(d));
    }
    public HashMap<String, Integer> readData(String path) throws Exception {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        String row = null;
        int count = 0;

        while ((row = reader.readLine()) != null) {
            String[] array = row.split(",");
           // Double dou= Double.parseDouble(array[1]);
                count++;
                int countForMap = map.getOrDefault(array[6], 0);
                countForMap++;
                map.put(array[6], countForMap);
                // System.out.println(array[1]);

//            count++;
            if (count == 98000) {
                break;
            }
        }

        return map;
    }

}
