package com.storm.proposedarchitecture;

import com.storm.countminsketch.Murmur3;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

public class FilterDesign {
	static PriorityBlockingQueue<PreFilteringModel> priorityQueue = new PriorityBlockingQueue<PreFilteringModel>(10);
	static short[] filter = new short[16];
	com.storm.countminsketch.CountMinSketch CMS = new com.storm.countminsketch.CountMinSketch(28, 4);
	boolean lastLayerCheck = false;



	public CBFModel CountingBloomFilter(String data, long counter) {
		CBFModel cbfandCMS = new CBFModel();
		short item = 0;
// Hashing of the DATA Item to find location in array
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
			CMS.setString(data, CMSCount);

			// Setting values to Counting Bloom Filter
			// it is added to the CMS and incremented with CBF
			if (CMSCount > (priorityQueue.peek().getNewCount())) {
				// estimated count
				long priorQue = priorityQueue.peek().getTotalCount();
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
				// lowerLayer++;
//System.out.println(lowerLayer);
			}

		}

		if ((filter[hashOne]) == filter[hashTwo]) {
			filter[hashOne] = (short) (filter[hashOne] + counter);
			filter[hashTwo] = (short) (filter[hashTwo] + counter);
			item = filter[hashTwo];
			cbfandCMS.setCountofCBF(item); // Set item frequency
			cbfandCMS.setLocation(hashTwo);// Set location of the item

		} else {

			if (filter[hashOne] > filter[hashTwo]) {

				filter[hashTwo] = (short) (filter[hashTwo] + counter);
				item = filter[hashTwo];
				cbfandCMS.setCountofCBF(item);
				cbfandCMS.setLocation(hashTwo);

			} else {

				filter[hashOne] = (short) (filter[hashOne] + counter);
				item = filter[hashOne];
				cbfandCMS.setCountofCBF(item);
				cbfandCMS.setLocation(hashTwo);
			}
		}
		return cbfandCMS;
	}
///First Method or method for main

	public boolean CounterDataStructure(List<Object> data) { // put value List<Object>
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
				itemSelection=true;
				long newCount = Pre.getNewCount() + 1;
				// new count is incremented
				long oldCount = Pre.getOldCount(); // Old count is same as priority queue have already contained

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
			itemSelection=false;
			if (priorityQueue.size() >= 32) {
				// TODO Auto-generated method stub
				CBFModel CBFFilter = new FilterDesign().CountingBloomFilter(item, 1);

				// add data element into the Counting Bloom Filter, See method
				// (CountingBloomFilter)
				if (!lastLayerCheck) {
					if ((CBFFilter.getCountofCBF()) > priorityQueue.peek().getNewCount()) {
						// filter[CBFFilter.getLocation()]=0;
						// System.out.println(CBFFilter.getCountofCBF()+"----"+priorityQueue.peek().getNewCount());

						PreFilteringModel preFilteringModel = new PreFilteringModel();
						preFilteringModel.setKey(item);
						preFilteringModel.setNewCount(CBFFilter.getCountofCBF());
						preFilteringModel.setOldCount(CBFFilter.getCountofCBF());

						new FilterDesign().CountingBloomFilter(priorityQueue.peek().getKey(),
								priorityQueue.peek().getTotalCount());
						priorityQueue.remove(priorityQueue.peek());
						priorityQueue.add(preFilteringModel);

					}
				}
			} else {
				PreFilteringModel preFilteringModel = new PreFilteringModel();
				preFilteringModel.setKey(item);
				preFilteringModel.setNewCount(1);
				preFilteringModel.setOldCount(0);
				priorityQueue.add(preFilteringModel);
			}

		}

		return itemSelection;

	}
}
