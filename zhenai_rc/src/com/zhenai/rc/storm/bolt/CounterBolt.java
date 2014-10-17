package com.zhenai.rc.storm.bolt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CounterBolt extends BaseBasicBolt {
 
	/**
	 * 
	 */ 
	private static final long serialVersionUID = -5508421065181891596L;
	
	private static long counter = 0;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {   
		try {  
			System.setOut(new PrintStream(new FileOutputStream(new File("/opt/storm/tmp/jar/flume-kafka-storm-inte-1_dis"), true)));
		} catch (FileNotFoundException e) {              
			e.printStackTrace();     
		}                      
		System.out.println("\n\n\nmsg = "+tuple.getString(0)+" -------------counter = "+(counter++));
		                               
	}   
 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
                              
	} 

}
