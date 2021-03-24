package com.storm.PreFilteringComparision;

import com.storm.countminsketch.CountMinSketch;
import com.storm.proposedarchitecture.PreFilteringModel;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class AugmentedSketch {
    static boolean itemCheckBoolean = false;
    static boolean itemForMiddleLayer=false;
    static PriorityBlockingQueue<PreFilteringModel> priorityQueue = new PriorityBlockingQueue<PreFilteringModel>(10);
    static CountMinSketch CMS = new CountMinSketch(28, 4);

    public void dataReading(String input, String output,int index) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));
        String row = null;
        int counter = 0;
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output)));
        StringBuilder builder = new StringBuilder();
        while ((row = reader.readLine()) != null) {
            String [] array= row.split(",");
            new AugmentedSketch().CounterDataStructure(array[0]);
        }
                for (int i = 0; i < CMS.getDepth(); i++)// for each row
                {
                    for (int j = 0; j < CMS.getWidth(); j++)// for each column

                    {
                        builder.append(CMS.multiset[i][j] + "");// append to the output string
                        if (j < CMS.getWidth() - 1)// if this is not the last row element
                            builder.append(",");// then add comma (if you don't like commas you can use spaces)
                    }
                    builder.append("\n");

            }
        writer.write(builder.toString());// save the string representation of the board
        writer.flush();
        writer.close();

        // break;
        }


    public void CounterDataStructure(String data) {
        if (priorityQueue == null) {

        }
        // System.out.println("Done");
        String item = data;

        Iterator<PreFilteringModel> iterator = priorityQueue.iterator();

        // System.out.println(iterator.next());
        while (iterator.hasNext()) {

            PreFilteringModel Pre = iterator.next();
            if ((Pre.getKey()).equals(item)) {
                long newCount = Pre.getNewCount() + 1;
                long oldCount = Pre.getOldCount(); // Old count is same as priority queue have already contained
                priorityQueue.remove(Pre); // Remove existing elements
                PreFilteringModel preFilterObj = new PreFilteringModel();
                preFilterObj.setKey(item);
                preFilterObj.setNewCount(newCount);
                preFilterObj.setOldCount(oldCount);
                // Adding object to the priorty queue
                priorityQueue.add(preFilterObj);
                itemCheckBoolean = true;
                itemForMiddleLayer=true;
                break;
            }
        }
        if(itemForMiddleLayer==false)
            if (priorityQueue.size() >= 32) {

                CMS.setString(item, 1);
                long minimumCountinCMS = CMS.getEstimatedCountString(item);
                if (minimumCountinCMS > priorityQueue.peek().getNewCount()) {
                    String key = priorityQueue.peek().getKey();
                    long countForCMS = priorityQueue.peek().getNewCount() - priorityQueue.peek().getOldCount();
                    priorityQueue.remove(priorityQueue.peek());
                    PreFilteringModel modelWhenCMSIsGreater = new PreFilteringModel();
                    modelWhenCMSIsGreater.setKey(item);
                    modelWhenCMSIsGreater.setNewCount(countForCMS);
                    modelWhenCMSIsGreater.setOldCount(countForCMS);
                    priorityQueue.add(modelWhenCMSIsGreater);
                    CMS.setString(key, countForCMS);
                }
                itemCheckBoolean = true;
            }
        // System.out.println(itemCheckBoolean+".."+item);
        if (itemCheckBoolean == false) {

            PreFilteringModel model = new PreFilteringModel();
            model.setKey(item);
            model.setNewCount(1);
            model.setOldCount(0);
            priorityQueue.add(model);


        }

    }
}
