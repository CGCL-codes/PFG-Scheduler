package com.ReadingFromDataBase;

import java.io.*;

public class LatencyCalculation {
//    public static void main(String[] args) throws  Exception {
//        new LatencyCalculation().latencyFinding(args[0],args[1]);
//    }
    public void latencyCalculation(String input, String output) throws Exception{
        double [] zipF={0.1,0.3,0.5,0.7,0.9,1.0,1.2,1.4,1.6,1.8,2.0};
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
        for(int i=0;i<zipF.length;i++){
           BufferedReader reader= new BufferedReader(new FileReader(new File(input+zipF[i]+".csv")));
           String row=null;
           int count=0;
           int counter=0;
           long aggregatedLatency=0;
           while ((row=reader.readLine())!=null){
               String[] array= row.split(",");
               try{
                   long time= Long.parseLong(array[1]);
                   if(time<0){
                       // System.out.println(time+" low Time--"+ count);
                       time=~time;
                       // break;
                   }
                   if(time>0){
                       aggregatedLatency=aggregatedLatency+time;
                   }
               }
               catch (NumberFormatException e){
                   aggregatedLatency=0;
               }
               count++;
               counter++;
               if(counter==32000000){
                   break;
               }
           }
           writer.write(zipF[i]+","+aggregatedLatency+","+count+"\n");
        }
        writer.flush();
        writer.close();
    }
    public void latencyFinding(String input, String output) throws Exception{
        double[] breakingPoint = { 100000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
                10000000, 12000000, 15000000, 20000000, 22000000, 25000000, 30000000};
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
        for(int i=0;i<breakingPoint.length;i++){
            BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
            String row=null;
            long aggregatedLatency=0;
            int count=0;
            int counter=0;
            while((row=reader.readLine())!=null){
                String[] array= row.split(",");
           //System.out.println(count+"----,"+array[1]);

                try {

                    long time= Long.parseLong(array[1]);
                    if(time<0){
                       // System.out.println(time+" low Time--"+ count);
                        time=~time;
                       // break;
                    }
                    if(time>0){
                        aggregatedLatency=aggregatedLatency+time;
                    }
                }
                catch (NumberFormatException e){

                    aggregatedLatency=0;
                }
                count++;
                if(count==breakingPoint[i]){
                    System.out.println(aggregatedLatency);
                    break;
                }
                }
            double aggregatedFrequency=aggregatedLatency/1000;
            writer.write(breakingPoint[i]+","+aggregatedFrequency+"\n");

        }
        writer.flush();
        writer.close();
    }
}
