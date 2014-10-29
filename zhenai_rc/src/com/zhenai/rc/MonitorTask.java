package com.zhenai.rc;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.zhenai.rc.domain.DataInfo;
import com.zhenai.rc.domain.DimColNameIdx;
import com.zhenai.rc.domain.IndiColNameIdx;
import com.zhenai.rc.domain.RCTask;
import com.zhenai.rc.storm.topology.RCTopologyDistri;
import com.zhenai.rc.util.CommonUtils;
import com.zhenai.rc.util.DBUtils;
import com.zhenai.rc.util.JSONUtil;
                                               
/*
    实际生产使用
   封装维度、指标的List
    
 1、定时扫描t_rc_task_*_info表，检查是否有新增的任务    
 2、启动一个Storm Topology，并将任务配置的参数传递给此Topology
   远程提交拓扑  submitTopoRemote     
   
 */   
public class MonitorTask {
	
	private static final Logger LOGGER = Logger.getLogger(MonitorTask.class); 
                                          	 
	private static Set<String> set_old = null; // 扫描前task_id组成的Set集合
	private static Set<String> set_new = new HashSet<String>();
	
	static { 
		set_new.add("1006001"); 
	}
	
	public static void main(String[] args) {
		timerWatchTask();                     
//		timerRun();             
	}                         
             
	private static void timerWatchTask() {
		Timer timer = new Timer("myTimer"); // 初始化一个定时器
		timer.schedule(new TimerTask() { 
			@Override    
			public void run() {    
				timerRun();                                  
			}             
	    // 5min一循环--1000*60*5                                                
		}, 0, 1000 * 10);         
		                                                                             
		while (true) {         
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
			try {
				Thread.sleep(1000);                         
			} catch (InterruptedException e) { 
				throw new RuntimeException(e);  
			}                                                                                        
		} // end of while()                                            
	}
	                                
	// 定时器运行的实际业务逻辑
	private static void timerRun() {                                
		if (set_new != null && set_new.size() > 0) { 
			set_old = new HashSet<String>(set_new); 
			set_new = DBUtils.getTaskIds();                                          
			                                                                                                                                                
			System.out.println("set_old: " + set_old);   
			System.out.println("set_new: " + set_new);
			
			// 前5分钟的task_id与这一刻的task_id，怎样比较多出来的task_id？  ----使用CommonUtils.getDiffent(*)取出新增任务的task_id   
			Collection differ = CommonUtils.getDiffent(set_old, set_new); 
			
			// 如果发现有新增的任务，就调用startNewTopo(*)，启动一个新的Topology
			if (differ != null && differ.size() > 0) {
				for (Object obj : differ) {
					String taskId = (String)obj;
					// rcTaskList	新增任务的配置信息
					// String taskId = "1006002";
					List<RCTask> rcTaskList = DBUtils.getTaskById(taskId);
					System.out.println("rcTaskList: " + rcTaskList); 
					// 有多少个新增的任务就起多少个Topology
					System.out.println("检测到新增的任务，任务id: " + taskId + "，提交一个新的Topology");
					startNewTopo(rcTaskList);                        
				}                                                                 
			} else {                                                 
				System.out.println("没有检测到新增的任务，不提交Topology\n"); 
			}                                                       
		}
	} 
 
	/*
	 * 发现有新增任务后，启动一个新的Topology                        
	 */
	private static void startNewTopo(List<RCTask> rcTaskList) {
		// 与data_info表进行对照，记录【字段1】在data_info中的顺序位置                   
		List<DataInfo> dataInfoList = new ArrayList<DataInfo>();
		String dataId = "t_fw_00002"; // t_fw_00002暂时写死，一般接口信息表data_info中的接口配置不会变                           
		dataInfoList = DBUtils.getDataInfoByDataId(dataId);   
		System.out.println("dataInfoList: " + dataInfoList); 		
		         
		// 参照rcTaskList，确定data_info中，哪些是维度、哪些是指标，并记录位置
		DimColNameIdx dimColNameIdx = null;                      
		IndiColNameIdx indiColNameIdx = null;                                           
		List<DimColNameIdx> dimList = new ArrayList<DimColNameIdx>();
		List<IndiColNameIdx> indiList = new ArrayList<IndiColNameIdx>();
		                                            
		// 找到此colName在接口信息表中的位置，并记录
		for (RCTask rcTask : rcTaskList) {      
			if (rcTask.getDataType() == 3) {
				String colName = rcTask.getColName();
				for (DataInfo dataInfo : dataInfoList) {
					if (dataInfo.getColName().equals(colName)) {
						int idx = dataInfoList.indexOf(dataInfo);
					    dimColNameIdx = new DimColNameIdx(colName, idx);  
					    dimList.add(dimColNameIdx);
					} 
				}
			} else if (rcTask.getDataType() == 4) {
				String colName = rcTask.getColName();
				for (DataInfo dataInfo : dataInfoList) {
					if (dataInfo.getColName().equals(colName)) {
						int idx = dataInfoList.indexOf(dataInfo);
						indiColNameIdx = new IndiColNameIdx(colName, idx);  
						indiList.add(indiColNameIdx);
					}          
				}
				
			}
		} // end of for()  
		                               
		System.out.println("--L144--dimList: " + dimList); 
		System.out.println("--L145--indiList: " + indiList);
		
		// 将dimList、indiList转换为JSON格式
		String dimListJson = JSONUtil.list2json(dimList);
		String indiListJson = JSONUtil.list2json(indiList);
		// 将配置信息传递给通用Topology
		RCTopologyDistri.submitTopoDistri(dimListJson, indiListJson);   
//		RCTopologyLocal.submitTopoLocal(dimListJson, indiListJson);   
		  
	}	
		
}
