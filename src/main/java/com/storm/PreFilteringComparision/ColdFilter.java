package com.storm.PreFilteringComparision;


import java.awt.*;
import java.io.*;
import java.util.HashMap;
import java.util.List;

import com.SpaceSaving.Counter;
import com.SpaceSaving.StreamSummary;
import com.storm.countminsketch.Murmur3;

public class ColdFilter {
	
	static int [] filterForFirstLayer= new int[32];
	static short[] filterForSecondLayer= new short[16];
	static StreamSummary<String> streamSummary = new StreamSummary<String>(0.001); // 1000
	

	public void Calculation(String input, String output) throws  Exception {
		BufferedReader reader= new BufferedReader(new FileReader(new File(input)));
		BufferedWriter writer= new BufferedWriter(new FileWriter(new File(output)));
		writer.write("Data, predictedCount, actualCount \n");
		HashMap<String, Integer> map= new HashMap<String, Integer>();
		String row=null;
		while((row=reader.readLine())!=null){
		int count=	map.getOrDefault(row,0);
			count++;
			map.put(row, count);
			int itemPrediction=new ColdFilter().firstFilter(row);
			writer.write(row+","+itemPrediction+","+map.get(row)+"\n");
		}
		writer.flush();
		writer.close();

	}
	public int  firstFilter(String key) {
		
		int[] hashNumbers= new ColdFilter().hashNumbers(key, filterForFirstLayer.length);
		//System.out.println(hash);
		int firstNumber=filterForFirstLayer[hashNumbers[0]];
		int secondNumber=filterForFirstLayer[hashNumbers[1]];
		int thirdNumber=filterForFirstLayer[hashNumbers[2]];
		//System.out.println(firstNumber+"--"+secondNumber+"--"+thirdNumber);
		if((firstNumber==secondNumber)&&(firstNumber==thirdNumber)) {
			if(firstNumber==Integer.SIZE*8/16) {
				//System.out.println(new ColdFilter().secondLayer(key));
				return new ColdFilter().secondLayer(key);
			}
			else {
				filterForFirstLayer[hashNumbers[0]]= firstNumber+1;
				filterForFirstLayer[hashNumbers[1]]=secondNumber+1;
				filterForFirstLayer[hashNumbers[2]]=thirdNumber+1;
				return filterForFirstLayer[hashNumbers[0]];
			}
		}
		else if((firstNumber==secondNumber)&&(firstNumber<thirdNumber)) {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[0]]= firstNumber+1;
			filterForFirstLayer[hashNumbers[1]]=secondNumber+1;
			}
			return filterForFirstLayer[hashNumbers[0]];
		}
		else if ((firstNumber==thirdNumber)&&(firstNumber<secondNumber)) {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[0]]= firstNumber+1;
			filterForFirstLayer[hashNumbers[2]]=thirdNumber+1;
			}
			return filterForFirstLayer[hashNumbers[0]];
		}
		else if ((secondNumber==thirdNumber)&&(secondNumber<firstNumber)) {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[1]]= secondNumber+1;
			filterForFirstLayer[hashNumbers[2]]=thirdNumber+1;
			}
			return filterForFirstLayer[hashNumbers[1]];
		}
		else if((firstNumber<secondNumber)&&(firstNumber<thirdNumber)) {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[0]]= firstNumber+1;
			}
			return filterForFirstLayer[hashNumbers[0]];
			
		}
		else if (secondNumber<thirdNumber) {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[1]]=secondNumber+1;
			}
			return filterForFirstLayer[hashNumbers[1]];
		} 
		else {
			if(firstNumber<=(Integer.SIZE*8/16)) {
			filterForFirstLayer[hashNumbers[2]]=thirdNumber+1;
			}
			return filterForFirstLayer[hashNumbers[2]];
		}
		//return 0;
		
		
	}
	public int  secondLayer(String Key) {
		int [] hashNumbers= new ColdFilter().hashNumbers(Key, filterForSecondLayer.length);
		short firstNumber=filterForSecondLayer[hashNumbers[0]];
		short secondNumber=filterForSecondLayer[hashNumbers[1]];
		short thirdNumber=filterForSecondLayer[hashNumbers[2]];
		
		if((firstNumber==secondNumber)&&(firstNumber==thirdNumber)) {
			int estimatedValue=0;
			if(firstNumber==Short.MAX_VALUE) {
				
				streamSummary.offer(Key);
				//System.out.println(streamSummary.getCapacity());
				//streamSummary.
				List<Counter<String>> listOfObjects = streamSummary.getTopK(1000);
				
				for(int i=0;i<listOfObjects.size();i++) {
					if(listOfObjects.get(i).getItem().equals(Key)) {
						//System.out.println("herre");
						estimatedValue =(int) listOfObjects.get(i).getValue()-
								(int)listOfObjects.get(i).getError();
						//System.out.println(estimatedValue);
						//break;
					return estimatedValue+32767;
						
					}
				}
				//return estimatedValue;
			}
			else {
				//System.out.println(firstNumber+"--"+secondNumber+"--"+thirdNumber);
				filterForSecondLayer[hashNumbers[0]]= (short) (firstNumber+1);
				filterForSecondLayer[hashNumbers[1]]=(short) (secondNumber+1);
				filterForSecondLayer[hashNumbers[2]]=(short) (thirdNumber+1);
				//System.out.println(filterForSecondLayer[hashNumbers[0]]);
				return filterForSecondLayer[hashNumbers[0]]+15;
				
			}
		}
		else if((firstNumber==secondNumber)&&(firstNumber<thirdNumber)) {
			if(firstNumber<=Short.MAX_VALUE) {
			filterForSecondLayer[hashNumbers[0]]= (short) (firstNumber+1);
			filterForSecondLayer[hashNumbers[1]]=(short) (secondNumber+1);
			
			return filterForSecondLayer[hashNumbers[0]]+15;
			}}
		else if ((firstNumber==thirdNumber)&&(firstNumber<secondNumber)) {
			if(firstNumber<=Short.MAX_VALUE) {
				
			filterForSecondLayer[hashNumbers[0]]= (short) (firstNumber+1);
			filterForSecondLayer[hashNumbers[2]]=(short) (thirdNumber+1);
			return filterForSecondLayer[hashNumbers[0]]+15;
			}}
		else if ((secondNumber==thirdNumber)&&(secondNumber<firstNumber)) {
			if(secondNumber<=Short.MAX_VALUE) {
				
			filterForSecondLayer[hashNumbers[1]]= (short) (secondNumber+1);
			filterForSecondLayer[hashNumbers[2]]=(short) (thirdNumber+1);
			return filterForSecondLayer[hashNumbers[1]]+15;
			}
		}
		else if((firstNumber<secondNumber)&&(firstNumber<thirdNumber)) {
			if(firstNumber<=Short.MAX_VALUE) {
				
			filterForSecondLayer[hashNumbers[0]]= (short) (firstNumber+1);
			
			return filterForSecondLayer[hashNumbers[0]]+15;
			}
		}
		else if (secondNumber<thirdNumber) {
			if(secondNumber<=Short.MAX_VALUE) {
			filterForSecondLayer[hashNumbers[1]]=(short) (secondNumber+1);
			return filterForSecondLayer[hashNumbers[1]]+15;
		} }
		else {
			if(thirdNumber<=Short.MAX_VALUE) {
				//System.out.println(filterForSecondLayer[hashNumbers[2]]+"--");
				
			filterForSecondLayer[hashNumbers[2]]=(short) (thirdNumber+1);
			return filterForSecondLayer[hashNumbers[2]]+15;
			}
		}
		return 0;
		//return 0;
		
				
	}
	
	public int[] hashNumbers(String data, int filterLength) {
		byte[] key= data.getBytes();
		long hash64 = Murmur3.hash64(key);
	    int hash1 = (int) hash64;
	    int hash2 = (int) (hash64 >>> 32);
	   // for (int i = 1; i <= d; i++) {
	      int combinedHash = hash1 + (1 * hash2);
	      // hashcode should be positive, flip all the bits if it's negative
	      if (combinedHash < 0) {
	        combinedHash = ~combinedHash;
	      }
	      int pos = combinedHash % filterLength;
	      hash1=hash1 % filterLength;
	      hash2=hash2 % filterLength;
	 
	      if(hash1<0) {
	    	  hash1=~hash1;
	      }
	      if(hash2<0) {
	    	  hash2=~hash2;
	      }
	    //
	      //System.out.println(pos+"--"+hash1+"--"+hash2);
	      int [] hashNumber= {pos,hash1,hash2};
	      return hashNumber;
	}

}
