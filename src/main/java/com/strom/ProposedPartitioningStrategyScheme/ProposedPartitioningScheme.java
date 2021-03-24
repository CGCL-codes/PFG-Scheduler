package com.strom.ProposedPartitioningStrategyScheme;

import com.storm.countminsketch.Murmur3;
import com.storm.proposedarchitecture.FilterDesign;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class ProposedPartitioningScheme implements CustomStreamGrouping {
	private List<Integer> choices;
	private AtomicInteger currentValue;
	private long[] targetTaskStats;
	private FilterDesign filterDesign;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.choices = targetTasks;
		// currentValue = new AtomicInteger(0);
		this.filterDesign = new FilterDesign();
		this.targetTaskStats = new long[this.choices.size()];


	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>(1);
		boolean itemSelection = filterDesign.CounterDataStructure(values);
		String data = values.get(0).toString();
		byte[] dataItem = data.getBytes();
		int[] hashFunction = new int[8];
		long hash64 = Murmur3.hash64(dataItem);
		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);
		if (hash1 < 0) {
			hash1 = ~hash1;
		}
		if (hash2 < 0) {
			hash2 = ~hash2;
		}
		if (!(itemSelection)) {

			hashFunction[0] = hash1 % (this.choices.size());
			hashFunction[1] = hash2 % (this.choices.size());
			int selected = targetTaskStats[hashFunction[0]] > targetTaskStats[hashFunction[1]] ? hashFunction[1]
					: hashFunction[0];
			boltIds.add(choices.get(selected));
			targetTaskStats[selected]++;

			return boltIds;
		} else {
			int combinedHash = 0;
			int i = 0;

			for (i = 2; i <= 7; i++) {
				combinedHash = hash1 + (i * hash2);
				combinedHash = (combinedHash) % this.choices.size();
				hashFunction[i] = combinedHash;
				// System.out.println(i);
				if (hashFunction[i] < 0) {
					hashFunction[i] = ~hashFunction[i];

				}
			}

			Arrays.parallelSort(hashFunction);
			int minimum = hashFunction[0];
			int maximum = hashFunction[7];
			// Random random = new Random();
			// DoubleStream rand=rn.doubles(minimum, maximum);

			int rand = ThreadLocalRandom.current().nextInt(minimum, maximum + 1);
			//System.out.println("Random" + rand);
			//System.exit(0);


			return Arrays.asList(choices.get(rand));

		}

	}
}
