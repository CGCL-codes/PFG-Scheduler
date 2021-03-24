package com.zipfgenerator;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class Zipfmain {
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();

	static String randomString( int len ) 
	{
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	static double roundTwoDecimals(double d) {
		  DecimalFormat twoDForm = new DecimalFormat("#.##");
		  return Double.valueOf(twoDForm.format(d));
		}
	
	public static void main(String[] args) {
		int numItems = 1000;
		int numUniqueItems = 100;
		String sep = "_";
		String distribution = "zipf";
		String directory  = "./Output/";
		//String directory ="/home/adeelaslam/ZipFData/";
		for (double skew = 0.1;skew<=2.0;skew+=0.1) {
			skew = roundTwoDecimals(skew);
			//System.out.println(skew+"Data");
			OutputWriter output = new OutputWriter(directory+distribution+sep+numItems+sep+numUniqueItems+sep+skew+".csv");
			List<String> list = new ArrayList<String>();
			
			ZipfGenerator zipf = new ZipfGenerator(numUniqueItems, skew);
			for(int i=1;i <= numUniqueItems; i++) { 
				long count = Math.round(zipf.getProbability(i)*numItems);
				//System.out.println(count+"ggg");
				String randomStr = randomString(20);
				for (int j=0;j<count;j++) {
					list.add(randomStr);
					//output.writeOutput(randomStr);
				}
			}
			
			Collections.shuffle(list,new Random(System.nanoTime()));
			//System.out.println(list.size());
			for (String str:list) {
				output.writeOutput(System.currentTimeMillis()/1000+"\t" + str);
			}
			output.closeOutput();
		}
	}
}
