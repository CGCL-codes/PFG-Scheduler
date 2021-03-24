package com.storm.proposedarchitecture;

import com.storm.countminsketch.Murmur3;

public class HashFunctions {

	 public void set(byte[] key) {
		    // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
		    // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
		    // implement a Bloom filter without any loss in the asymptotic false positive probability'
		    // The paper also proves that the same technique (using just 2 pairwise independent hash functions)
		    // can be used for Count-Min sketch.

		    // Lets split up 64-bit hashcode into two 32-bit hashcodes and employ the technique mentioned
		    // in the above paper
		    long hash64 = Murmur3.hash64(key);
		    int hash1 = (int) hash64;
		    int hash2 = (int) (hash64 >>> 32);
		    for (int i = 1; i <= 10; i++) {
		      int combinedHash = hash1 + (i * hash2);
		      // hashcode should be positive, flip all the bits if it's negative
		      if (combinedHash < 0) {
		        combinedHash = ~combinedHash;
		      }
		     System.out.println(combinedHash);
		     
		    }
		  }

}
