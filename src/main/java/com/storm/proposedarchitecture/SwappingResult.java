package com.storm.proposedarchitecture;
import com.storm.PreFilteringComparision.AugmentedSketch;
import com.storm.PreFilteringComparision.ColdFilter;
import com.storm.countminsketch.Murmur3;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

public class SwappingResult {
    static PriorityBlockingQueue<PreFilteringModel> priorityQueue = new PriorityBlockingQueue<PreFilteringModel>(10);
    static short[] filter = new short[16];
    static com.storm.countminsketch.CountMinSketch CMS = new com.storm.countminsketch.CountMinSketch(20, 3);
    static boolean lastLayerCheck = false;

    static BufferedWriter writer;
//    static int countForSwappingCBF;
//    static int countForSwppingCMS;

    public static void main(String[] args) throws Exception{
       // int index=Integer.parseInt(args[2]);
        new AugmentedSketch().dataReading(args[0],args[1],Integer.parseInt(args[2]));
    }
    public void dataCalculation(String input, String output) throws  Exception{
        BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
        BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
        HashMap<String, Integer> map= new HashMap<>();
        writer.write("data,PredictedCount, ActualCount \n");
        String row= null;
        while((row=reader.readLine())!=null){
            List<Object> list= new ArrayList<>();
            list.add(row);
            new SwappingResult().CounterDataStructure(list);
            int count= map.getOrDefault(row,0);
            count++;
            map.put(row,count);
           // writer.write(row+","+countForFilter+","+map.get(row)+"\n");
              }
       // writer.write(countForSwappingCBF+","+countForSwppingCMS+"\n");
        writer.flush();
        writer.close();
    }
    public CBFModel CountingBloomFilter(String data, long counter) throws Exception {
        CBFModel cbfandCMS = new CBFModel();
        short item = 0;
        long hash64 = Murmur3.hash64(data.getBytes());
        int hashOne = ((int) hash64) % 16;
        int hashTwo = ((int) (hash64 >>> 32)) % 16;
        if (hashTwo < 0) {
            hashTwo = ~hashTwo;
        }
        if (hashOne < 0) {
            hashOne = ~hashOne;
        }
        lastLayerCheck = false;
        // Loop to check that all items in the array for overflow
        if ((filter[hashOne] == Short.MAX_VALUE) && (filter[hashTwo] == Short.MAX_VALUE)) {
            lastLayerCheck = true;
            long CMSCount = CMS.getEstimatedCountString(data) > 0 ? 1 : (Short.MAX_VALUE + 1);
            //countForFilter=CMSCount;
            CMS.setString(data, CMSCount);
            if (CMSCount > (priorityQueue.peek().getNewCount())) {
                // estimated count
                //countForSwppingCMS++;
                long priorQue = priorityQueue.peek().getNewCount()-priorityQueue.peek().getOldCount();
                String prioString = priorityQueue.peek().getKey();
                // Top key of the priority key
                CMS.setString(prioString, (priorQue));
                // Set new string with key and its new_count
                PreFilteringModel PFM = new PreFilteringModel();// New Object
                PFM.setKey(data);
                PFM.setNewCount(CMSCount); // CMS estimated count
                PFM.setOldCount(CMSCount);
                priorityQueue.remove(priorityQueue.peek()); // Remove from the queue existing peek
                priorityQueue.add(PFM);// Add new object

            }

        }

        if ((filter[hashOne]) == filter[hashTwo]) {
            if(filter[hashOne]<=Short.MAX_VALUE) {
                filter[hashOne] = (short) (filter[hashOne] + counter);
                filter[hashTwo] = (short) (filter[hashTwo] + counter);
                item = filter[hashTwo];

                cbfandCMS.setCountofCBF(item); // Set item frequency
                cbfandCMS.setLocation(hashTwo);// Set location of the item
            }

        } else {

            if (filter[hashOne] > filter[hashTwo]) {
                // writer.write(data+","+filter[hashTwo] + "CBF, hashOneGreater"+"\n");
                if(filter[hashTwo]<=Short.MAX_VALUE) {
                    filter[hashTwo] = (short) (filter[hashTwo] + counter);
                    item = filter[hashTwo];
                    cbfandCMS.setCountofCBF(item);
                    cbfandCMS.setLocation(hashTwo);
                }

            } if(filter[hashTwo]>filter[hashOne]) {
                if(filter[hashOne]<=Short.MAX_VALUE) {
                    // writer.write(data+","+filter[hashOne] +"CBF,hashTwoGreater"+ "\n");
                    filter[hashOne] = (short) (filter[hashOne] + counter);
                    item = filter[hashOne];
                    cbfandCMS.setCountofCBF(item);
                    cbfandCMS.setLocation(hashTwo);
                }}
        }
        return cbfandCMS;
    }

    public boolean CounterDataStructure(List<Object> data) throws Exception { // put value List<Object>

        boolean itemSelection = false;
        if (priorityQueue == null)
            return false;

        // data
        String item = data.get(0).toString(); /// Data Item

        boolean check = false; /// For Check for Item if exist in the priority queue or not in priority queue

        Iterator<PreFilteringModel> iterator = priorityQueue.iterator();
        Label1: while (iterator.hasNext()) {

            PreFilteringModel Pre = iterator.next();
            // If new coming item exists in the priority queue
            if ((Pre.getKey()).equals(item)) {
                itemSelection = true;
                long newCount = Pre.getNewCount() + 1;
                // new count is incremented
                long oldCount = Pre.getOldCount(); // Old count is same as priority queue have already contained
                //countForFilter=newCount-oldCount;
                priorityQueue.remove(Pre); // Remove existing elements
                // Add that element with new old and new count.
                PreFilteringModel preFilterObj = new PreFilteringModel();
                preFilterObj.setKey(item);
                preFilterObj.setNewCount(newCount);
                preFilterObj.setOldCount(oldCount);
                // Adding object to the priorty queue
                priorityQueue.add(preFilterObj);
                // System.out.println("added ");
                check = true; // Check True means if element not exist
                // System.out.println(Pre.getKey()+"--"+ item);

                break Label1;

            }
        }

        if (!check) { // check size of priority queue
            itemSelection = false;
            if (priorityQueue.size() >= 32) { // 32
                // TODO Auto-generated method stub
                CBFModel CBFFilter = new SwappingResult().CountingBloomFilter(item, 1);
             //   countForFilter=CBFFilter.getCountofCBF();
                if (!lastLayerCheck) {
                    if ((CBFFilter.getCountofCBF()) > priorityQueue.peek().getNewCount()) {
                      //countForSwappingCBF++;
                        PreFilteringModel preFilteringModel = new PreFilteringModel();
                        preFilteringModel.setKey(item);
                        preFilteringModel.setNewCount(CBFFilter.getCountofCBF());
                        preFilteringModel.setOldCount(CBFFilter.getCountofCBF());
                        long totalCount= priorityQueue.peek().getNewCount()-priorityQueue.peek().getOldCount();
                        new SwappingResult().CountingBloomFilter(priorityQueue.peek().getKey(),
                                totalCount);
                        priorityQueue.remove(priorityQueue.peek());
                        priorityQueue.add(preFilteringModel);

                    }
                }
            } else {
                PreFilteringModel preFilteringModel = new PreFilteringModel();
                // writer.write(0+"\n");
                preFilteringModel.setKey(item);
                preFilteringModel.setNewCount(1);
                preFilteringModel.setOldCount(0);
                //countForFilter=preFilteringModel.getNewCount();
                priorityQueue.add(preFilteringModel);
            }

        }


        return itemSelection;

    }
}
