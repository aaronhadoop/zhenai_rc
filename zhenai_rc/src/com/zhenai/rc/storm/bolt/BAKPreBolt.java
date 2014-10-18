package com.zhenai.rc.storm.bolt;
  
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zhenai.rc.util.DBUtils;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
         
/*
    功能：
 接口过滤   
 1、字段选取
 2、前置条件处理 
 3、维度收拢
 */ 
public class BAKPreBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	                   
	private static Map<String, String> storeMap = null; // 维度指标相对应
	private static List<String> distinctList = null; // 去重计算 
	private static long counter = 0;   
	        
	private static Map<String, Double> maxMap = null;
	private static Map<String, Double> minMap = null;
	  
	// 从conf中取任务的配置信息     
	private static List<String> rcTaskVO1List = null;
	private static List<String> rcTaskVO2List = null;
	private static List<String> rcTaskVO3List = null;
	private static List<String> rcTaskVO4List = null;
	private static List<String> zks = null;  
	    
    @Override  
    public void prepare(Map conf, TopologyContext context) {
    	storeMap = new HashMap<String, String>();    
    	distinctList = new ArrayList<String>(); 
    	     
    	zks = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
    	rcTaskVO1List = (List<String>) conf.get("rcTaskVO1List");
    	rcTaskVO2List = (List<String>) conf.get("rcTaskVO2List");
    	rcTaskVO3List = (List<String>) conf.get("rcTaskVO3List");
    	rcTaskVO4List = (List<String>) conf.get("rcTaskVO4List");
    	
    	maxMap = new HashMap<String, Double>();  
	    minMap = new HashMap<String, Double>();  
    }                   
	                                                             
	@Override     
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("\n1---------------start---------------");
		/*
		    rcTaskVO1List: [1,oper_type,=1]
			rcTaskVO2List: [2,login_time,0] 
			rcTaskVO3List: [3,pf,1,(1,2),(3,4)]  
			rcTaskVO4List: [4,user_id,0,0, 4,user_id,1,0, 4,pay_amt,2,0]
		 */  
		System.out.println("2zks: " + zks);        
		System.out.println("3###rcTaskVO1List: " + rcTaskVO1List);
		System.out.println("4###rcTaskVO2List: " + rcTaskVO2List);
		System.out.println("5###rcTaskVO3List: " + rcTaskVO3List);
		System.out.println("6###rcTaskVO4List: " + rcTaskVO4List);
		                             
		try {                                                                                                                              
			System.setOut(new PrintStream(new FileOutputStream(new File("E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_PreBolt_result"), true)));
			System.out.println("\n7PreBolt_msg: "+tuple.getString(0) + " -------------counter = " + (counter++));     
			// one tuple one record                                    
			String record = tuple.getString(0);               
		                                                                         	              
			if(record != null && record.length() > 0) {                   
				String[] fields = record.split("\t");     
				// 过滤脏数据，5代表t_fw_00001接口下对应的字段数量                                                
				if (fields.length == 5) {                                     
					 // 过滤接口，只取t_fw_00001的数据  
					 if (fields[0].equals("t_fw_00001")) {   
						// 过滤oper_type，只取oper_type为1的数据
						if (fields[2].equals("1")) {    
							List<Object> values = tuple.getValues();   
             				                                           	 	 	 
							String str = (String)values.get(0);	 
							String[] splits = str.split("\t");   
							     	 	   
							String pf = splits[1];
							if (Integer.valueOf(pf) == 1 || Integer.valueOf(pf) == 2) {
								pf ="12";  
							} else if(Integer.valueOf(pf) == 3 || Integer.valueOf(pf) == 4) {
								pf ="34";
							}   	                            
							            	
							                  
							String oper_type = splits[2];     	  
							String user_id = splits[3];  
							double pay_amt = Double.valueOf(splits[4]);
System.out.println("\n8pf: " + pf + ",oper_type: " + oper_type + ",user_id: " + user_id + ",pay_amt: " + pay_amt);
							System.out.println("\n9computingIdx start...");
							// 指标计算COUNT SUM MAX                                                                
							computingIdx(pf, oper_type, user_id, pay_amt);       
							System.out.println("10computingIdx over..."); 
						}                           
					}   
				}   
			}                        
			System.out.println("---------------end---------------\n");
			
		} catch (Exception e) {      
			e.printStackTrace();  
		}        
	}                            
	
                                     
	// 核心函数  指标计算  
	private void computingIdx(String pf, String oper_type, String user_id, Double pay_amt) { // key--pf		value--COUNT(user_id),COUNT(DISTINCT(user_id)),SUM(pay_amt)
		 // 求max、min
		 String key2 = pf;       
		 Double max_pay_amt = maxMap.get(key2);
		 Double min_pay_amt = minMap.get(key2);
		                 
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
				 double avg_pay_amt = sum_pay_amt / count_dis_user_id;    
//				 double avg_pay_amt = sum_pay_amt / count_dis_user_id; 
//				 max_pay_amt = max_pay_amt(pay_amt);          
//				 min_pay_amt = min_pay_amt(pay_amt);       
				            
				 // 最大值
//				 String key2 = pf;     
//				 Double max_pay_amt = maxMap.get(key2);  
				 System.out.println("10----max_pay_amt: " + max_pay_amt);
//				 if (max_pay_amt == null) {          
//					 max_pay_amt = Double.MIN_VALUE;
//					 maxMap.put(pf, max_pay_amt);  
//				 }                                                                          
				 if (pay_amt > max_pay_amt) {     
					 max_pay_amt = pay_amt;
				 }                                                            
				 maxMap.put(pf, max_pay_amt);               
				                                                                                                                                                                                       	
				 // 最小值                                         	   		 
//				 Double min_pay_amt = maxMap.get(key2);
//				 if (min_pay_amt == null) {  
//					 min_pay_amt = Double.MAX_VALUE;
//					 maxMap.put(pf, Double.MAX_VALUE);
//				 }       
				 if (pay_amt < min_pay_amt) {   
					 min_pay_amt = pay_amt;  
				 }
				 maxMap.put(pf, min_pay_amt);   
				 System.out.println("10----min_pay_amt: " + min_pay_amt);
				  
				 val = count_user_id + "," + count_dis_user_id + "," + sum_pay_amt + "," + max_pay_amt + "," + min_pay_amt + "," + avg_pay_amt;  
			 } else {               
				val = 1 + "," + (isHasValue ? 0 : 1) + "," + pay_amt  + "," + 0  + "," + 0 + "," + 0;   
				max_pay_amt = pay_amt;              
				min_pay_amt = pay_amt; 
				                   
				maxMap.put(pf, max_pay_amt);          
				minMap.put(pf, min_pay_amt);              
			}        
			 System.out.println("pf: " + pf + ", " + "val: " + val);
			 storeMap.put(pf, val);           
			 System.out.println("*****storeMap: " + storeMap);
			     
			    
			 for(Map.Entry<String, String> entry : storeMap.entrySet()) {
				 String k = entry.getKey();  
				 String v = entry.getValue();   
				 System.out.println(k + " -- " + v); 
				 // TODO a_计算结果入库 
				 DBUtils.insertToDB(k,v);        
			 }                                  
		}                           
	}
   

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		     
	}
	          
	// 检查storeMap是否为null
	private boolean checkMap() {
		if(storeMap == null){   
			storeMap = new HashMap<String, String>();
		}
		return true;
	}     
	          
	// 求最大金额      
//	private double max_pay_amt(Double pay_amt) {
//		if (pay_amt > max_pay_amt) { 
//			max_pay_amt = pay_amt;
//		}    
//		return max_pay_amt;       
//	}        
                                                                    
	// 求最小金额                   
//	private double min_pay_amt(Double pay_amt) {
//		if (pay_amt < min_pay_amt) {
//			min_pay_amt = pay_amt;
//		}  
//		return min_pay_amt;      
//	}
	
}
