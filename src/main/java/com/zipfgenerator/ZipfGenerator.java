package com.zipfgenerator;

import java.util.Random;

public class ZipfGenerator {
	private Random rnd = new Random(System.currentTimeMillis());
	private int size;
	private double skew;
	private double bottom = 0;
 
	public ZipfGenerator(int size, double skew) {
		this.size = size;
		this.skew = skew;
 
		for(int i=1;i < size; i++) {
			this.bottom += (1/Math.pow(i, this.skew));
			
		}
		System.out.println(this.bottom+"   b");
	}
 
	// the next() method returns a random rank id.
	// The frequency of returned rank ids are follows Zipf distribution.
	public int next() {
		int rank;
		double frequency = 0;
		double dice;
 
		rank = rnd.nextInt(size);
		frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
		dice = rnd.nextDouble();
 
		while(!(dice < frequency)) {
			rank = rnd.nextInt(size);
			frequency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
			dice = rnd.nextDouble();
		}
		return rank;
	}
 
	// This method returns a probability that the given rank occurs.
	public double getProbability(int rank) {
		return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
	}
 
}