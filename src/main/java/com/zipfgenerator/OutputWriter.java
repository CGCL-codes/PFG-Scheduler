package com.zipfgenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class OutputWriter {
    BufferedWriter output;
    OutputWriter(String fileName) {
	try {
	    output = new BufferedWriter(new FileWriter(fileName),1000);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    void writeOutput (String str) {
	try {
	    output.write(str+"\n");
	   
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	
    }
    void closeOutput() {
	try {
	    output.flush();
	    output.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
}
