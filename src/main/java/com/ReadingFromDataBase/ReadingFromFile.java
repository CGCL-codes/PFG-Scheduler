package com.ReadingFromDataBase;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class ReadingFromFile {
    public static void main(String[] args) throws Exception {

     new ReadingFromFile().fileWriting(args[0],args[1],args[2]);

    }
    static double roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#.##");
        return Double.valueOf(twoDForm.format(d));
    }
    public void fileWriting(String inputFile, String outPut,String pathforCount) throws Exception{
        int[] array = { 100000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000,
	    10000000, 12000000, 15000000, 20000000, 22000000, 25000000, 27999850};
       // ArrayList<Integer> list= new ReadingFromFile().dataForCount(pathforCount);
   //     System.out.println(list.size());
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(outPut)));
        for(int i=0;i<array.length;i++){
            HashMap<String, Integer> map= readingDataSet(inputFile,array[i]);
            Iterator it = map.entrySet().iterator();
            double dataAverage = 0;
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                Object item= pair.getValue();
                dataAverage = dataAverage +  (Integer)item;
            }

            dataAverage = dataAverage / 4;

            int max = Collections.max(map.values());
           writer.write(array[i]+"," + roundTwoDecimals(max - dataAverage) + "\n");
            System.out.println(i);
        }
        writer.flush();
        writer.close();
    }
    public ArrayList<Integer> dataForCount(String path) throws Exception{
        ArrayList<Integer> list= new ArrayList<Integer>();
        BufferedReader reader= new BufferedReader(new FileReader(new File("/home/adeelaslam/DataCount/"+path)));
        String row=null;
        while((row=reader.readLine())!=null){
            int data= Integer.parseInt(row);
            list.add(data);
        }
        return list;
    }
    public static int[] sampleRandomNumbersWithoutRepetition(int start, int end, int count) {
        Random rng = new Random();
        // int count =10000000;
        int[] result = new int[count];
        int cur = 0;
        int remaining = end - start;
        for (int i = start; i < end && count > 0; i++) {
            double probability = rng.nextDouble();
            if (probability < ((double) count) / (double) remaining) {
                count--;
                result[cur++] = i;
            }
            remaining--;
        }
        return result;
    }

    public HashMap<String, Integer> readingDataSet(String inputFile,int breakingPoint) throws Exception{
        HashMap<String,Integer> map= new HashMap<String, Integer>();
        String row= null;
        int counter=0;
        BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)));
        while ((row=reader.readLine())!=null){
            String [] array= row.split(",");
            int count= map.getOrDefault(array[4],0);
                count++;
                map.put(array[4],count);
                counter++;
                if(counter==breakingPoint){
                    break;
                }
        }
        reader.close();
        return  map;
    }
}
