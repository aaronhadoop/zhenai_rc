package com.zhenai.rc.storm.spout;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
 
/* 
  读取模拟数据源（inputPath）中的数据 
 */
public class GetDataSpout extends BaseRichSpout {
  
	private static final long serialVersionUID = 1L;
	private String inputPath;
	private SpoutOutputCollector collector;
	private static Integer cnt = 0;
	
	
	  
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    this.collector = collector;  
	    this.inputPath = (String)conf.get("INPUT_PATH"); 	  
	}
           
	@Override
	public void nextTuple() {    
	    Collection files = FileUtils.listFiles(new File(this.inputPath), FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")), null);
//	    for (File f : files)    
	    for (Object f : files)            
	      try {            
	        List lines = FileUtils.readLines((File)f, "UTF-8");
//	        for (String line : lines)
	        for (Object line : lines) {                                                   
	        	System.out.println("\n\n----GetDataSpout_line:	" + line + " ---- cnt: " + (cnt++));
	        	// 逐行发送 
	        	this.collector.emit(new Values(new Object[] { line }));
	        }                     
	     // change the name of the file  
	        FileUtils.moveFile((File) f, new File(((File) f).getPath() + System.currentTimeMillis() + ".bak")); 
	      } catch (IOException e) { 
	        e.printStackTrace();
	      }               
	  }   
	 
    
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields(new String[] { "line" }));
	}
 
}
