package com.zhenai.rc.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.zhenai.rc.domain.DataInfo;
import com.zhenai.rc.domain.RCTask;

public class DBUtils {
	private static final String dbconfig = "dbconfig.properties" ;
	private static Properties prop = new Properties() ;
	
	private static Connection conn = null;
	private static Statement stmt = null;
	
	static {
		init();    
	}     

	/**
	 *              
	* @name init
	* @description 初始化操作
	* @param  
	* @return void    
	* @throws
	 */
	private static void init() {  
		try { 
			InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(dbconfig);
			prop.load(in); 
//			Class.forName(prop.getProperty("driverClassName"));
			Class.forName("com.mysql.jdbc.Driver");  
			                                                          
			conn = DBUtils.getConnection();  
			stmt = conn.createStatement();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("类没找到", e);                             
		} catch (SQLException e) {                       
			throw new RuntimeException(e);        
		}
	} 

	public static void main(String[] args) {
//		System.out.println(DBUtils.getDataInfoByDataId("t_fw_00002"));
		System.out.println(DBUtils.getTaskIds());
	}
	
	/**
	 * 
	* @name getConnection
	* @description 获取数据库连接
	* @param  
	* @return Connection    
	* @throws                
	 */
	public static Connection getConnection () {
		try {
			if(conn == null){
				return DriverManager.getConnection(prop.getProperty("url"),
						prop.getProperty("username"), "123456");
			}          
			return conn;                 
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	  
	/*
	 * 返回zhenai.rc_task_conf中所有task_id的Set集合
	 */     
	public static Set<String> getTaskIds() {
		Set<String> set = new HashSet<String>();
		 try {     
				String sql = "SELECT task_id FROM rc_task_conf;";
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String task_id = rs.getString("task_id");
					set.add(task_id);
				}
				return set;
		  } catch (Exception e) {     
			  throw new RuntimeException("getTaskIds() error...", e);
		  }
	}
  
	/*
	 * 通过task_id查找某一实时任务 
	 * 一个task_id对应多条记录
	 */
	public static ArrayList<RCTask> getTaskById(String taskId) {
		ArrayList<RCTask> rcTaskList = new ArrayList<RCTask>();
		RCTask rcTask = null;
		 try {
				String sql = "SELECT * FROM rc_task_conf WHERE task_id=" + taskId;
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) { 
					Integer fkId = rs.getInt("fk_id");
					String taskIdTmp = rs.getString("task_id");
					Integer dataType = rs.getInt("data_type");
					String colName = rs.getString("col_name");
					String preCondition = rs.getString("pre_condition");
					Integer timeInerval = rs.getInt("time_inerval");
					Integer isCollapsed = rs.getInt("is_collapsed");
					String collapsedRule = rs.getString("collapsed_rule");
					Integer indicatorOper = rs.getInt("indicator_oper");
					Integer indicatorOperType = rs.getInt("indicator_oper_type");
					String resColName = rs.getString("res_col_name");
					
					rcTask = new RCTask(fkId, taskIdTmp, dataType, colName, preCondition, timeInerval, 
							                   isCollapsed, collapsedRule, indicatorOper, indicatorOperType, resColName);
					rcTaskList.add(rcTask);
				}
				return rcTaskList;     
		  } catch (Exception e) {
			  throw new RuntimeException("通过task_id查找某一实时任务 失败", e);
			}
	}

	/*
	 * 通过dataId从rc_data_info接口信息表中获取某一接口	
	 */
	public static List<DataInfo> getDataInfoByDataId(String dataId) {
		List<DataInfo> dataInfoList = new ArrayList<DataInfo>();
		DataInfo dataInfo = null;
		 try {
				String sql = "SELECT * FROM rc_data_info WHERE data_id='" + dataId + "'";
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {  
//					String dataId = rs.getString("data_id");
					String colName = rs.getString("col_name");
					String comment = rs.getString("comment");
					
					dataInfo = new DataInfo(dataId, colName, comment);
					dataInfoList.add(dataInfo); 
				}
				return dataInfoList;     
		  } catch (Exception e) {
			  throw new RuntimeException("通过dataId查找某一接口数据 失败", e);
			}
	}	
	
	/*
	 *将处理后结果存入数据库
	 */
	public static void insertToDB(String pf, String val) {
		 String[] split = val.split(",");
		 if (split.length == 6) {  
			 try {
				 String sql = "INSERT INTO rc_result VALUES(?,?,?,?,?,?,?);"; 
				 PreparedStatement pstmt = conn.prepareStatement(sql);
				 pstmt.setInt(1, Integer.valueOf(pf));
				 pstmt.setInt(2, Integer.valueOf(split[0])); 
				 pstmt.setInt(3, Integer.valueOf(split[1]));  
				 pstmt.setDouble(4, Double.valueOf(split[2]));  
				 pstmt.setDouble(5, Double.valueOf(split[3]));
				 pstmt.setDouble(6, Double.valueOf(split[4])); 
				 pstmt.setDouble(7, Double.valueOf(split[5]));   
				 pstmt.execute(); 
				 System.out.println("insertToDB successfully...");
			} catch (SQLException e) {   
				throw new RuntimeException("insertToDB failed", e);
			}         
	 }        	 
	}
	
             
	public static void insertToDBSimple(String k, String v) {
		 try { 
			 String sql = "INSERT INTO rc_result_simple VALUES(?,?);"; 
			 PreparedStatement pstmt = conn.prepareStatement(sql);
			 pstmt.setString(1, k);  
			 pstmt.setInt(2, Integer.valueOf(v));   
			 pstmt.execute();                          
			 System.out.println("insertToDBSimple successfully...");
		} catch (SQLException e) {   
			throw new RuntimeException("insertToDB failed", e);
		}		
	} 
	
}
