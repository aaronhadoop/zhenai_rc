package com.zhenai.rc.storm.topology;
    
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
 
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.zhenai.rc.storm.bolt.BAKPreBolt;
import com.zhenai.rc.storm.spout.GetDataSpout;
      
public class RCTopology {        
	private static final Log log = LogFactory.getLog(RCTopology.class);
	        
	public static void main(String[] args) {    
		try {                                  	    	                               
		TopologyBuilder builder = new TopologyBuilder();
//      builder.setSpout("sendDataSpout", new SendDataSpout(), 2);
//		builder.setBolt("preBolt", new PreBolt(),1).shuffleGrouping("sendDataSpout");
		                               
        builder.setSpout("getDataSpout", new GetDataSpout(), 2);
        builder.setBolt("preBolt", new BAKPreBolt(),1).shuffleGrouping("getDataSpout");
                   
        // inputPath 模拟数据源                                                  
//        String inputPath = "/opt/storm/tmp/jar/zhenai_rc_source_data_dir";           
        String inputPath = "E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_source_data_dir";           
        Config config = new Config();                                                              
        config.setNumWorkers(2);        
        config.put("INPUT_PATH", inputPath);      
        config.setDebug(true);         
                       
        log.warn("--------submitting topology--------");
        //  + CommonUtils.dateToString(new Date()) 
//		 StormSubmitter.submitTopology("RCTopology", config, builder.createTopology());
        LocalCluster cluster = new LocalCluster();     
        cluster.submitTopology("RCTopology", config, builder.createTopology());
		log.warn("--------topology submitted--------");    
		      
		} catch (Exception e) {   
			throw new RuntimeException(e);
		}
	}
}
