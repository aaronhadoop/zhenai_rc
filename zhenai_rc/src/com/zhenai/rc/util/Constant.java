package com.zhenai.rc.util;

/*
 * dbconfig.properties
 */
public class Constant {  
  	
	// MySQL
	public static String MYSQL_DRIVER="com.mysql.jdbc.Driver";
	public static String MYSQL_URL ="jdbc:mysql://192.168.131.142:3306/zhenai?useUnicode=true&characterEncoding=utf-8";
	public static String MYSQL_USERNAME="root"; 
	public static String MYSQL_PASSWORD="123456";
	                               
	// Memcached                                                                                                    
	public static String MEMCACHE_HOST_PORT="slave2:11212";
	public static int[] MEMCACHE_WEIGHT=new int[]{1};
	         
}