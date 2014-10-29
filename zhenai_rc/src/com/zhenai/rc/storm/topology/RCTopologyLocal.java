package com.zhenai.rc.storm.topology;
    
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.zhenai.rc.domain.DimColNameIdx;
import com.zhenai.rc.domain.IndiColNameIdx;
import com.zhenai.rc.storm.bolt.SimplestPreBolt;
import com.zhenai.rc.storm.spout.GetDataSpout;

/*
 测试Topology
 */
public class RCTopologyLocal {        
	private static final Log log = LogFactory.getLog(RCTopologyLocal.class);
	
	public static void main(String[] args) {
		List<DimColNameIdx> dimList = new ArrayList<DimColNameIdx>();
		List<IndiColNameIdx> indiList = new ArrayList<IndiColNameIdx>();
		dimList.add(new DimColNameIdx("pf", 0));
		dimList.add(new DimColNameIdx("age", 1));
		indiList.add(new IndiColNameIdx("user_id", 2));
		 
		submitTopoLocal(dimList, indiList);                    
	}                                                        
	                      
	public static void submitTopoLocal(List<DimColNameIdx> dimList, List<IndiColNameIdx> indiList) {
		try {                                  	    	                               
		TopologyBuilder builder = new TopologyBuilder();
//      builder.setSpout("sendDataSpout", new SendDataSpout(), 2);
//		builder.setBolt("preBolt", new PreBolt(),1).shuffleGrouping("sendDataSpout");
		                               
        builder.setSpout("getDataSpout", new GetDataSpout(), 2);
        builder.setBolt("SimplestPreBolt", new SimplestPreBolt(),1).shuffleGrouping("getDataSpout");
                                               
        // inputPath 模拟数据源                                                                                                                                                                                                      
//        String inputPath = "/opt/storm/tmp/jar/zhenai_rc_source_data_dir";           
        String inputPath = "E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_source_data_dir";           
        Config config = new Config();                                                                  
        config.setNumWorkers(2);        
        config.put("INPUT_PATH", inputPath);       
        config.setDebug(true);    
                                 
        config.put("dimList", dimList);  
        config.put("indiList", indiList);   
                       
        System.out.println("--------submitting topology--------");                   
        //  + CommonUtils.dateToString(new Date()) 
//		 StormSubmitter.submitTopology("RCTopology", config, builder.createTopology());
        LocalCluster cluster = new LocalCluster();       
        System.out.println("\nconfig: " + config);                                           
        cluster.submitTopology("RCTopology", config, builder.createTopology());
        System.out.println("--------topology submitted--------");    
		                                                              
		} catch (Exception e) {               
			throw new RuntimeException(e);
		}
	}   
}
