package com.zhenai.rc.storm.bolt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zhenai.rc.util.PreBoltHelper;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
  
/*
 实际生产中的bolt
 
    功能：
 接口过滤   
 1、字段选取
 2、前置条件处理
 3、维度收拢
 */
public class PreBolt extends BaseBasicBolt {
    
	private static final long serialVersionUID = 1L;
	private static Map<String, String> storeMap = null; // 维度指标相对应
	private static List<String> distinctList = null; // 去重计算 
	private static long counter = 0;   
	private static double max_pay_amt_tmp = Double.MIN_VALUE; // 计算max、min   
	private static double min_pay_amt_tmp = Double.MAX_VALUE;   
	  
	// 从conf中取任务的配置信息                
	private static List<String> rcTaskVO1List = new ArrayList<String>();
	private static List<String> rcTaskVO2List = new ArrayList<String>();
	private static List<String> rcTaskVO3List = new ArrayList<String>();
	private static List<String> rcTaskVO4List = new ArrayList<String>();
	private static List<String> zks = null;  
	 
    @Override         
    public void prepare(Map conf, TopologyContext context) {
    	System.out.println("\n\n################# PreBolt prepare() method invoked");
    	storeMap = new HashMap<String, String>();     
    	distinctList = new ArrayList<String>(); 
                                                       
		/*
		    rcTaskVO1List: [1,oper_type,=1]
			rcTaskVO2List: [2,login_time,0] 
			rcTaskVO3List: [3,pf,1,(1,2),(3,4)]
			rcTaskVO4List: [4,user_id,0,0, 4,user_id,1,0, 4,pay_amt,2,0]
	    */  
//    	zks = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
//    	rcTaskVO1List = (List<String>) conf.get("rcTaskVO1List");
//    	rcTaskVO2List = (List<String>) conf.get("rcTaskVO2List");
//    	rcTaskVO3List = (List<String>) conf.get("rcTaskVO3List");
//    	rcTaskVO4List = (List<String>) conf.get("rcTaskVO4List");
    	                
    	// 实时计算任务配置
    	rcTaskVO1List.add("1,oper_type,=1");       
    	rcTaskVO2List.add("2,login_time,0");        

    	// 默认5个维度
    	rcTaskVO3List.add("3	pf	1	(1,2)&(3,4)"); 
    	rcTaskVO3List.add("3	age	1	(10,20),(20,30),(30,40)");
    	rcTaskVO3List.add("3	age	1	100");
    	rcTaskVO3List.add("3	age	1	100");
    	rcTaskVO3List.add("3	age	1	100");
    	
    	rcTaskVO4List.add("4,user_id,0,0");           
    	rcTaskVO4List.add("4,user_id,1,0");     
    	rcTaskVO4List.add("4,pay_amt,2,0");             
    }                              
	  
	@Override  
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("\n---------------start---------------");
	 	       
		System.out.println("zks: " + zks); 
		System.out.println("###rcTaskVO1List: " + rcTaskVO1List);
		System.out.println("###rcTaskVO2List: " + rcTaskVO2List);
		System.out.println("###rcTaskVO3List: " + rcTaskVO3List);
		System.out.println("###rcTaskVO4List: " + rcTaskVO4List);
		                        
		try {                                                                                                                      
			System.setOut(new PrintStream(new FileOutputStream(new File("E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_PreBolt_result"), true)));
			System.out.println("\nPreBolt_msg: "+tuple.getString(0) + " -------------counter = " + (counter++));     
			// one tuple one record  
			// 一行记录中的字段顺序已按配置表中字段顺序排列，且字段数相等 
			String record = tuple.getString(0);           
			                                              
			if(record != null && record.length() > 0) {              
				String[] fields = record.split("\t");      
				// 过滤脏数据，5代表t_fw_00001接口下对应的字段数量                
				if (fields.length == 5) {           
					 // 过滤接口，只取t_fw_00001的数据，这一步会由Kafka Consumer来做    
					 if (fields[0].equals("t_fw_00001")) { 
						 /*     
						  1、前置条件处理      
						  */                    
						 // 过滤oper_type，只取oper_type为1的数据   
						 // 获取前置条件【=1】             
						 String preCondition = rcTaskVO1List.get(0).split(",")[2];
						 // 使用前置条件 		  
						 String preCondition1 = preCondition.substring(preCondition.length() - 2, preCondition.length() - 1);
						 String preCondition2 = preCondition.substring(preCondition.length() - 1, preCondition.length());
						 // 转化"=1" 
						 if (preCondition1.equals("=")) { 
							// Integer.parseInt(fields[2])  取出数据源中的前置条件（oper_type） 
							 if (Integer.parseInt(fields[2]) == Integer.parseInt(preCondition2)) { // 取oper_type=1的数据   
									List<Object> values = tuple.getValues();  
System.out.println("values: " + values);
									String str = (String)values.get(0);	 
									String[] splits = str.split("\t");   
									/*
									 2、 时间维度           	 	   
									 3、 维度处理     
									 */        
									// 取出数据源中的维度
									String pf = splits[1];
									// 取出任务配置中的维度
									              
									    
									// 使用任务配置中的维度
									      
//								if (Integer.valueOf(pf) == 1 || Integer.valueOf(pf) == 2) {
								if (PreBoltHelper.isContains(rcTaskVO3List, pf)) {
										pf ="12";                      
							    } else if(PreBoltHelper.isContains2(rcTaskVO3List, pf)) {
										pf ="34";      
							    }      	                             
								      
    								// t_fw_00001      3       1       1001    100.1
									String oper_type = splits[2];               	    
									String user_id = splits[3];                               
									double pay_amt = Double.valueOf(splits[4]);                  
		System.out.println("\npf: " + pf + ",oper_type: " + oper_type + ",user_id: " + user_id + ",pay_amt: " + pay_amt);      
									System.out.println("\ncomputingIdx start...");
									/*     
									 4、指标计算                                  
									 */             
									computingIdx(pf, oper_type, user_id, pay_amt);  
									System.out.println("computingIdx over..."); 
								}
							
						 } else if (preCondition1.equals(">")) {
							             
						 } else if (preCondition1.equals("<")) {
							 
						}
          
					}      
				}   
			}                       
			System.out.println("---------------end---------------\n");
			
		} catch (Exception e) {   
			e.printStackTrace(); 
		}       
	}        
       
	// 核心函数  指标计算  五种类型都计算
	private void computingIdx(String pf, String oper_type, String user_id, Double pay_amt) { // key--pf		value--COUNT(user_id),COUNT(DISTINCT(user_id)),SUM(pay_amt)
		 String key = pf + user_id;                                      
		 boolean isHasValue = false;           
		 if(distinctList.contains(key)) {            
			isHasValue = true;            
		 } else {                                            
			 distinctList.add(key);               
		 }                                
	                                                                                                                                              
		 if (checkMap()) {                          
			 String val = storeMap.get(pf);                                      
			 if(val != null) {                  
				 String[] vals = val.split(",");    
				 int count_user_id = Integer.valueOf(vals[0]) + 1;
				 int count_dis_user_id = Integer.valueOf(vals[1]) + (isHasValue ? 0 : 1); 
				 double sum_pay_amt = Double.valueOf(vals[2]) + pay_amt;  
//				 double avg_pay_amt = sum_pay_amt / count_dis_user_id; 
				 double max_pay_amt = max_pay_amt(pay_amt);           
				 double min_pay_amt = min_pay_amt(pay_amt);          
				           
				 val = count_user_id + "," + count_dis_user_id + "," + sum_pay_amt + "," + max_pay_amt + "," + min_pay_amt;  
			 } else {         
				val = 1 + "," + (isHasValue ? 0 : 1) + "," + pay_amt + 0  + "," + 0;
			}       
			 System.out.println("pf: " + pf + ", " + "val: " + val);
			 storeMap.put(pf, val);  
			 System.out.println("*****storeMap: " + storeMap);
			       
			   
			   
			 for(Map.Entry<String, String> entry : storeMap.entrySet()) {
				 String k = entry.getKey();      
				 String v = entry.getValue();   
				 System.out.println(k + " -- " + v); 
				 // TODO a_计算结果入库
			 }           
		}        
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	////////////////
	private boolean checkMap() {
		if(storeMap == null){  
			storeMap = new HashMap<String, String>();
		}
		return true;
	}
	
	// 求最大金额      
	private double max_pay_amt(Double pay_amt) {
		if (pay_amt > max_pay_amt_tmp) {
			max_pay_amt_tmp = pay_amt;
		}
		return max_pay_amt_tmp;
	}          
              
	// 求最小金额                   
	private double min_pay_amt(Double pay_amt) {
		if (pay_amt < min_pay_amt_tmp) {
			min_pay_amt_tmp = pay_amt;
		}  
		return pay_amt;      
	}
}
