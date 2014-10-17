package com.zhenai.rc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
 
import com.zhenai.rc.domain.RCTask;
import com.zhenai.rc.storm.bolt.PreBolt;
import com.zhenai.rc.storm.spout.GetDataSpout;
import com.zhenai.rc.util.DBUtils;
   
/*
 1、定时扫描t_rc_task_*_info表，检查是否有新增的任务    
 2、启动一个Storm Topology，并将任务配置的参数传递给此Topology

 读取任务的配置表
 
远程提交拓扑  submitTopoRemote
 */   
public class MonitorTask {
	private static Set set_old = null; 
	private static Set set_new = null;
	    
	
	public static void main(String[] args) {
		timerWatchTask();
	}                         
             
	private static void timerWatchTask() {
		Timer timer = new Timer("myTimer");
		timer.schedule(new TimerTask() { 
			@Override    
			public void run() {    
				System.out.println("run a topology...");

				if (set_new != null && set_new.size() > 0) {
					set_old = new HashSet<String>(set_new);
				}
				set_new= DBUtils.getTaskIds();
				// 前5分钟的task_id与这一刻的task_id，怎样比较多出来的task_id   
				System.out.println("set_old: " + set_old);
				System.out.println("set_new: " + set_new);
				  
				// rcTaskList  新增任务的配置信息
				List<RCTask> rcTaskList = DBUtils.getTaskById((String)set_new.toArray()[0]);
				System.out.println("---: " + rcTaskList); 
				submitTopoRemote(rcTaskList);
				   
				
				// 发现有新增的任务才进行以下逻辑处理
//				if (set_old == null || set_old.size() == 0) {
//					System.out.println("set_old为null");   
//				}
//				if (set_old != null && set_old.size() > 0) {
//					System.out.println("set_new新增的元素: " + CommonUtils.getDiffent(set_old, set_new));  
//					Collection differ = CommonUtils.getDiffent(set_old, set_new);
//				
//					if (differ != null && differ.size() > 0) { // 如果发现有新增的任务
//						for (Object obj : differ) {
//							System.out.println("(String)obj: " + (String)obj);
//							List<RCTask> rcTaskList = DBUtils.getTaskById((String)obj);
//							Map map = new HashMap<String, String>();
//							      
//							
//							
////						submitTopology(map); // 将新增任务的配置信息传递给通用的Topology 
//						}
//					} else {
//						System.out.println("no topology added");
//					}
//					
//
//				}
			}
                 
		}, 0, 1000); // 5min循环--1000*300       
		
		
		
		while (true) {  
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) { 
				e.printStackTrace();  
			}
		}
	} // end of fun1()

	// 提交Topology
	private static void submitTopoRemote(List<RCTask> rcTaskList) {
		try { 
			// 1.1 创建builder，设置Spout、Bolt
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("getDataSpout", new GetDataSpout(), 2);
			builder.setBolt("preBolt", new PreBolt(),1).shuffleGrouping("getDataSpout");
		    // 1.2 设置conf   	   	 	
			Config conf = new Config();          
			conf.setDebug(true);     
			conf.setNumWorkers(3);        
			conf.put(Config.NIMBUS_HOST, "192.168.131.134"); //配置nimbus连接主机地址，比如：192.168.10.1
			String[] STORM_ZOOKEEPER_SERVERS = {"192.168.131.134","192.168.131.141","192.168.131.142"}; 
			conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS)); //配置zookeeper连接主机地址，可以使用集合存放多个
	        String inputPath = "/opt/storm/tmp/jar/zhenai_rc_source_data_dir";           
//	        String inputPath = "E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_source_data_dir";           
	        conf.put("INPUT_PATH", inputPath); 
                       
			// 配置任务start 
			List<String> rcTaskVO1List = new ArrayList<String>();
			List<String> rcTaskVO2List = new ArrayList<String>();
			List<String> rcTaskVO3List = new ArrayList<String>();
			List<String> rcTaskVO4List = new ArrayList<String>();
                                                               
			for (RCTask rcTask : rcTaskList) { 
				if (rcTask.getDataType() == 1) {  
//					RCTaskVO1 rcTaskVO1 = new RCTaskVO1(rcTask.getDataType(), rcTask.getColName(), rcTask.getPreCondition());
					rcTaskVO1List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getPreCondition());    
				} else if (rcTask.getDataType() == 2) {
//					RCTaskVO2 rcTaskVO2 = new RCTaskVO2(rcTask.getDataType(), rcTask.getColName(), rcTask.getTimeInerval());       
					rcTaskVO2List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getTimeInerval());
				} else if (rcTask.getDataType() == 3) {
//					RCTaskVO3 rcTaskVO3 = new RCTaskVO3(rcTask.getDataType(), rcTask.getColName(), rcTask.getIsCollapsed(), rcTask.getCollapsedRule());
//					rcTaskVO3List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getIsCollapsed() + "," + rcTask.getCollapsedRule());
					rcTaskVO3List.add(rcTask.getDataType() + "\t" + rcTask.getColName() + "\t" + rcTask.getIsCollapsed() + "\t" + rcTask.getCollapsedRule());
				} else if (rcTask.getDataType() == 4) {            
//					RCTaskVO4 rcTaskVO4 = new RCTaskVO4(rcTask.getDataType(), rcTask.getColName(), rcTask.getIndicatorOper(), rcTask.getIndicatorOperType());      
					rcTaskVO4List.add(rcTask.getDataType() + "," + rcTask.getColName() + "," + rcTask.getIndicatorOper() + "," + rcTask.getIndicatorOperType());           
				}              
			}  
			System.out.println("rcTaskVO1List: " + rcTaskVO1List);
			System.out.println("rcTaskVO2List: " + rcTaskVO2List);
			System.out.println("rcTaskVO3List: " + rcTaskVO3List);
			System.out.println("rcTaskVO4List: " + rcTaskVO4List);
			
//			List<String> rcTaskVO1ListString = new ArrayList<String>();
//			rcTaskVO1ListString.add("11111");  
//			conf.put("rcTaskVO1List", rcTaskVO1ListString);  
//			System.out.println("-----: " + rcTaskVO1ListString);
		  	
			conf.put("rcTaskVO1List", rcTaskVO1List); // 目的所在
			conf.put("rcTaskVO2List", rcTaskVO2List);
			conf.put("rcTaskVO3List", rcTaskVO3List);
			conf.put("rcTaskVO4List", rcTaskVO4List); 
			           
			System.out.println("conf: " + conf); 
			// 配置任务end
                  
			//storm默认使用System.getProperty("storm.jar")去取  
			System.setProperty("storm.jar","E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc.jar");
			// 1.3 提交Topology      
//			StormSubmitter.submitTopology("RemoteSubmitTopo-3-zhenai_rc", conf, builder.createTopology());
		} catch (Exception e) {                                             
			e.printStackTrace();
		}         
	} 
	
}
