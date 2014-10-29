package com.zhenai.rc.storm.topology;

import java.util.Arrays;

import com.zhenai.rc.storm.bolt.PreBolt;
import com.zhenai.rc.storm.spout.GetDataSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;

/*
 * 远程提交Topology【定时器触发】
 * 通过代码提交Topology 
 */
public class RemoteSubmitTopoTest {
	  public static void main(String[] args) throws Exception {
		    TopologyBuilder builder = new TopologyBuilder();
  
	        builder.setSpout("getDataSpout", new GetDataSpout(), 2);
	        builder.setBolt("preBolt", new PreBolt(),1).shuffleGrouping("getDataSpout");
		      	 	
		    Config conf = new Config();
		    conf.put(Config.NIMBUS_HOST,"192.168.131.134"); //配置nimbus连接主机地址，比如：192.168.10.1
		    String[] STORM_ZOOKEEPER_SERVERS = {"192.168.131.134","192.168.131.141","192.168.131.142"}; 
		    conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS)); //配置zookeeper连接主机地址，可以使用集合存放多个
		    conf.setDebug(true);                  
		    conf.setNumWorkers(3);     
                                      
		    //storm默认使用System.getProperty("storm.jar")去取 
		    System.setProperty("storm.jar","E:\\0_tmp\\data\\zhenai_rc\\lifeCycle.jar");
		    StormSubmitter.submitTopology("RemoteSubmitTopo-2-lifeCycle", conf, builder.createTopology());
		  }                
}
