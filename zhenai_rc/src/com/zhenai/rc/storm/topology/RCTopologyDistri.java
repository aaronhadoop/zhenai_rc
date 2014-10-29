package com.zhenai.rc.storm.topology;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.zhenai.rc.storm.bolt.SimplestPreBolt;
import com.zhenai.rc.storm.spout.GetDataSpout;

public class RCTopologyDistri {  
	
		private static final Logger LOGGER = Logger.getLogger(RCTopologyDistri.class); 

		public static void main(String[] args) {  
			submitTopoDistri(null, null);  
		}
		      
	    /*            
	     * 提交Topology，将任务配置信息传递给Topology                               
	     */ 
		public static void submitTopoDistri(String dimListJson,String indiListJson) {
//		public static void submitTopoDistri(List<DimColNameIdx> dimList, List<IndiColNameIdx> indiList) {
//		public static void submitTopoRemote(ArrayList<RCTask> rcTaskList) {
			try {                         
				      
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("getDataSpout", new GetDataSpout(), 2);
				builder.setBolt("preBolt", new SimplestPreBolt(),1).shuffleGrouping("getDataSpout");
			                                       	     	 	
				Config conf = new Config();                                      
				conf.setDebug(true);                      
				conf.setNumWorkers(3);  
				// 192.168.131.134 10.10.10.175                        
				conf.put(Config.NIMBUS_HOST, "10.10.10.175");          
				// {"10.10.10.175","10.10.10.177","10.10.10.179"}           
				// {"192.168.131.134","192.168.131.141","192.168.131.142"}
				String[] STORM_ZOOKEEPER_SERVERS = {"10.10.10.175","10.10.10.177","10.10.10.179"}; 
				conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS)); 
		        String inputPath = "/opt/storm/tmp/jar/zhenai_rc_source_data_dir";             
//		        String inputPath = "E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_source_data_dir";           
		        conf.put("INPUT_PATH", inputPath);                       
	                                                                                                           
				// 配置任务start                                         
				List<String> rcTaskVO1List = new ArrayList<String>();
				List<String> rcTaskVO2List = new ArrayList<String>();
				List<String> rcTaskVO3List = new ArrayList<String>();
				List<String> rcTaskVO4List = new ArrayList<String>();
                				                                                                                                                                                         
				                                                                                                  
				// 1.1、一行记录，需要判断哪些字段是前置条件、时间、维度、指标
//	            for (RCTask rcTask : rcTaskList) {
//					                  
//					if (rcTask.getDataType() == 1) {  
//						RCTaskVO1 rcTaskVO1 = new RCTaskVO1(rcTask.getDataType(), rcTask.getColName(), rcTask.getPreCondition());
//						rcTaskVO1List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getPreCondition());    
//					} else if (rcTask.getDataType() == 2) {
//						RCTaskVO2 rcTaskVO2 = new RCTaskVO2(rcTask.getDataType(), rcTask.getColName(), rcTask.getTimeInerval());       
//						rcTaskVO2List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getTimeInerval());
//					} else if (rcTask.getDataType() == 3) {
//						RCTaskVO3 rcTaskVO3 = new RCTaskVO3(rcTask.getDataType(), rcTask.getColName(), rcTask.getIsCollapsed(), rcTask.getCollapsedRule());
//						rcTaskVO3List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getIsCollapsed() + "," + rcTask.getCollapsedRule());
//						rcTaskVO3List.add(rcTask.getDataType() + "\t" + rcTask.getColName() + "\t" + rcTask.getIsCollapsed() + "\t" + rcTask.getCollapsedRule());
//					} else if (rcTask.getDataType() == 4) {            
//						RCTaskVO4 rcTaskVO4 = new RCTaskVO4(rcTask.getDataType(), rcTask.getColName(), rcTask.getIndicatorOper(), rcTask.getIndicatorOperType());      
//						rcTaskVO4List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getIndicatorOper() + "," + rcTask.getIndicatorOperType());           
//					}               
//	            }                 
//				System.out.println("rcTaskVO1List: " + rcTaskVO1List);
//				System.out.println("rcTaskVO2List: " + rcTaskVO2List);
//				System.out.println("rcTaskVO3List: " + rcTaskVO3List);
//				System.out.println("rcTaskVO4List: " + rcTaskVO4List);
				   
//				List<String> rcTaskVO1ListString = new ArrayList<String>();
//				rcTaskVO1ListString.add("11111");  
//				conf.put("rcTaskVO1List", rcTaskVO1ListString);  
//				System.out.println("-----: " + rcTaskVO1ListString);
			  	
//				conf.put("rcTaskVO1List", rcTaskVO1List);  
//				conf.put("rcTaskVO2List", rcTaskVO2List);
//				conf.put("rcTaskVO3List", rcTaskVO3List);
//				conf.put("rcTaskVO4List", rcTaskVO4List);  
				
				conf.put("dimListJson", dimListJson);  
				conf.put("indiListJson", indiListJson);  
				                                                                                                                                                                  
				conf.put("con_key_test", "con_value_test");         
				                                     
				LOGGER.debug("conf: " + conf);      
				// 配置任务end
	                                                                        
				//storm默认使用System.getProperty("storm.jar")去取  
//				System.setProperty("storm.jar","E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc.jar");
				String suffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());                                        
				StormSubmitter.submitTopology("RCTopologyDistri" + suffix, conf, builder.createTopology());
			} catch (Exception e) {                                                                                           
				throw new RuntimeException(e);                                                                                                  
			}                                               
		}                           

}

