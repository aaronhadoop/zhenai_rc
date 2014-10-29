package com.zhenai.rc.util;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.zhenai.rc.domain.RCTask;

public class CommonUtils {

	public static String dateToString(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(date);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Collection getDiffent(Collection collmax, Collection collmin) {
		// 使用LinkeList防止差异过大时,元素拷贝
		Collection csReturn = new LinkedList();
		Collection max = collmax;
		Collection min = collmin;
		// 先比较大小,这样会减少后续map的if判断次数
		if (collmax.size() < collmin.size()) {
			max = collmin;
			min = collmax;
		}
		// 直接指定大小,防止再散列
		Map<Object, Integer> map = new HashMap<Object, Integer>(max.size());
		for (Object object : max) {
			map.put(object, 1);
		}
		for (Object object : min) {
			if (map.get(object) == null) {
				csReturn.add(object);
			} else {
				map.put(object, 2);
			}
		}
		for (Map.Entry<Object, Integer> entry : map.entrySet()) {
			if (entry.getValue() == 1) {
				csReturn.add(entry.getKey());
			}
		}
		return csReturn;
	}

	/**
	 * 获取两个集合的不同元素,去除重复
	 *  
	 * @param collmax
	 * @param collmin
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
//	public static Collection getDiffentNoDuplicate(Collection collmax, Collection collmin) {
//		return new HashSet(getDiffent(collmax, collmin));
//	}
	
	public static void main(String[] args) {
		Set<String> set_old = new HashSet<String>();
		set_old.add("1006001");
		
		Set<String> set_new = new HashSet<String>();
		set_new.add("1006001");
		set_new.add("1006002");                                            
		
		Collection differ = CommonUtils.getDiffent(set_old, set_new);
		System.out.println("differ: " + differ);
		
		if (differ != null && differ.size() > 0) { // 如果发现有新增的任务
			for (Object obj : differ) { 
				System.out.println("(String)obj: " + (String)obj);
				List<RCTask> rcTaskList = DBUtils.getTaskById((String)obj);
				System.out.println("rcTaskList: " + rcTaskList);
//				Map map = new HashMap<String, String>(); 
//			submitTopology(map); // 将新增任务的配置信息传递给通用的Topology 
			}                  
		}
	}

	 
	
}

