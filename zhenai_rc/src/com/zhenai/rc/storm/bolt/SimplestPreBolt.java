package com.zhenai.rc.storm.bolt;
  
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zhenai.rc.domain.DimColNameIdx;
import com.zhenai.rc.domain.IndiColNameIdx;
import com.zhenai.rc.util.DBUtils;

/*   
 * 多维度测试             
 */ 
public class SimplestPreBolt extends BaseBasicBolt { 
	private static final long serialVersionUID = 1L;
	       
	// 记录发送过来的数据条数     
	private static int counter = 0;
	
	/*
	 *  存放维度、指标的元数据信息，如：
	 *  dimList: [ColNameIdx [colName=pf, idx=0], ColNameIdx [colName=age, idx=1]]
	 *  indiList: [ColNameIdx [colName=user_id, idx=2]]
	 */        
	List<DimColNameIdx> dimList = new ArrayList<DimColNameIdx>();
	List<IndiColNameIdx> indiList = new ArrayList<IndiColNameIdx>();
	
	// 维度--COUNT(指标)                                   
	private static Map<String, String> storeMap = null;  
	private static int keyTimes = 0; // COUNT       
                                  
                   	
	// 从conf中取任务的配置信息     
//	private static List<String> rcTaskVO1List = new ArrayList<String>();
//	private static List<String> rcTaskVO2List = new ArrayList<String>();
//	private static List<String> rcTaskVO3List = new ArrayList<String>();
//	private static List<String> rcTaskVO4List = new ArrayList<String>();
	
	public static void main(String[] args) {
		new SimplestPreBolt().execute(null, null);
	}
	            
    @Override  
    public void prepare(Map conf, TopologyContext context) {
    	storeMap = new HashMap<String, String>();    
    	 
//    	dimList = (List<DimColNameIdx>)conf.get("dimList");
//    	indiList = (List<IndiColNameIdx>)conf.get("indiList");
    	
    	Gson gson = new Gson();            
    	              
    	String dimListJson = (String)conf.get("dimListJson");
    	Type typeOfSrcDim = new TypeToken<List<DimColNameIdx>>(){}.getType();
    	dimList = gson.fromJson(dimListJson, typeOfSrcDim);
    	                                         
    	String indiListJson = (String)conf.get("indiListJson");
    	Type typeOfSrcIndi = new TypeToken<List<IndiColNameIdx>>(){}.getType();
    	indiList = gson.fromJson(indiListJson, typeOfSrcIndi);     	
    	                       
    	
//    	rcTaskVO1List = (List<String>) conf.get("rcTaskVO1List");
//    	rcTaskVO2List = (List<String>) conf.get("rcTaskVO2List");
//    	rcTaskVO3List = (List<String>) conf.get("rcTaskVO3List");
//    	rcTaskVO4List = (List<String>) conf.get("rcTaskVO4List");
                            	 
    	// 实时计算任务配置    
//    	rcTaskVO1List.add("1,oper_type,=1");          
//    	rcTaskVO2List.add("2,login_time,0");         
  
    	// 默认5个维度
//    	rcTaskVO3List.add("3	pf	1	(1,2)&(3,4)"); 
//    	rcTaskVO3List.add("3	age	1	(10,20),(20,30),(30,40)");
//    	   
//    	rcTaskVO4List.add("4,user_id,0,0");             
//    	rcTaskVO4List.add("4,user_id,1,0");       
//    	rcTaskVO4List.add("4,pay_amt,2,0");               
    }                             
	                                                                                             
	@Override                                                                      
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("\n---------------start---------------");
		
//		System.out.println("3###rcTaskVO1List: " + rcTaskVO1List);
//		System.out.println("4###rcTaskVO2List: " + rcTaskVO2List);
//		System.out.println("5###rcTaskVO3List: " + rcTaskVO3List);
//		System.out.println("6###rcTaskVO4List: " + rcTaskVO4List);
		                                                    
		try {                                                                                                                                         
//			System.setOut(new PrintStream(new FileOutputStream(new File("E:\\0_tmp\\data\\zhenai_rc\\zhenai_rc_PreBolt_result"), true)));
			System.out.println("\n##########新数据: " + tuple.getString(0) + " -------counter = " + (counter++));     
				                    
				    // str--获取数据源中的一条记录                                                                                                                                                      
				 	String sourceData = (String)tuple.getValue(0);	   					 
//			        String sourceData = "1	20	1001"; 
			            
					String[] array = sourceData.split("\t");         
                   
					///模拟数据start///
					// 定义List<DimColNameIdx>存放在Topology中设置到conf中的维度信息
//					List<DimColNameIdx> dimList = new ArrayList<DimColNameIdx>();
//					dimList.add(new DimColNameIdx("pf", 0));
//					dimList.add(new DimColNameIdx("age", 1));
//					 
//					List<IndiColNameIdx> indiList = new ArrayList<IndiColNameIdx>();
//					indiList.add(new IndiColNameIdx("user_id", 2));
					///模拟数据end///					 
					
					
					List<String> dimListData = new ArrayList<String>(); // 存放维度位置所在字段的数据
					List<String> indiListData = new ArrayList<String>(); // 存放指标
					                                                                 
//					dimListData.add(array[0]); // array[?]             
//					dimListData.add(array[1]);                                                    
					for (DimColNameIdx dimColNameIdx : dimList) {  
						dimListData.add(array[dimColNameIdx.getIdx()]);                          
					}                                                  
					                                  
					for (IndiColNameIdx indiColNameIdx : indiList) {    
						indiListData.add(array[indiColNameIdx.getIdx()]);                             
					}
				    System.out.println("dimListData: " + dimListData);                                
				    System.out.println("indiListData: " + indiListData);                                
					                    
//					// 默认按5个维度处理
//					if (dimListData.size() < 5) {  
//						dimListData.add(new String("0"));
//						dimListData.add(new String("0"));
//						dimListData.add(new String("0"));
//					}
					
					// 将源数据按维度、指标分开
//					indi_list.add(array[2]);                             
				              
			           
					  
					System.out.println("\n9computingIdx start...");
					// computingIdx(pf, oper_type, user_id, pay_amt);         
					/*
					 * dimListData	维度数据所在字段组成的List
					 * indiListData	指标数据所在字段组成的List
					 */               
					computingIdx(dimListData, indiListData);       
					System.out.println("10computingIdx over..."); 
					                   
				                       
			                                          
			System.out.println("---------------end---------------\n");
		} catch (Exception e) {       
			e.printStackTrace();  
		}        
	}                            
	
	/*
	 *	dimListData	维度
	 *	indiListData	指标
	 */
	private void computingIdx(List<String> dimListData, List<String> indiListData) { // key--pf		value--COUNT(user_id),COUNT(DISTINCT(user_id)),SUM(pay_amt)
		  StringBuffer key = new StringBuffer("flag,");
		  for (int i = 0; i < dimListData.size(); i++) {
			  if (i == dimListData.size() - 1) {
				  key = key.append(dimListData.get(i));
				  continue; 
			}                                      
			  key = key.append(dimListData.get(i)).append(",");
		  } 
		 System.out.println("----key: " + key);
		                                        
		 if (checkMap()) {           
			                                                            
			 String value = storeMap.get(key);                                                   
			 if(value != null) {                        
//				 String[] vals = value.split(",");        
//				 int count_user_id = Integer.valueOf(vals[0]) + 1;
//				 value = String.valueOf(count_user_id);  
				 
				 // 如果map中存在这个key
				
				 keyTimes = Integer.valueOf(value);   
				 keyTimes ++;                                                        
			 } else {               
				 keyTimes = 1;                 
			}                         
			 storeMap.put(key.toString(), String.valueOf(keyTimes));           
			 System.out.println("*****storeMap: " + storeMap);            
			                                        
			 // 写入数据库                   
			 for(Map.Entry<String, String> entry : storeMap.entrySet()) {
				 String k = entry.getKey();   
				 String v = entry.getValue();                   
				 System.out.println(k + " -- " + v);  
				 
				 
				 
				 // 写数据库之前应该有一个操作，实现COUNT
				 DBUtils.insertToDBSimple(k,v);                           
			 }                                                
		}                           
	} 
	 
	           
	// 检查storeMap是否存在，不存在则创建    
	private boolean checkMap() {     
		if(storeMap == null) {       
			storeMap = new HashMap<String, String>();
		}
		return true;                          
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	} 
	
}

