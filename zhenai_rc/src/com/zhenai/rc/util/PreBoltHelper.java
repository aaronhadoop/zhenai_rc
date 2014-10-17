package com.zhenai.rc.util;

import java.util.ArrayList;
import java.util.List;


public class PreBoltHelper {
	
	public static boolean isContains(List<String> rcTaskVO3List, String pf) {
		String[] split = rcTaskVO3List.get(0).split("\t");
		String s = split[split.length - 1];
		
		String[] s2 = s.split("&");
		
		List l = new ArrayList();
		for(int i = 0; i < s2[0].length(); i ++) {
			l.add(s2[0].charAt(i));
		} 
		return l.contains(pf.charAt(0));
	}

	public static boolean isContains2(List<String> rcTaskVO3List, String pf) {
		String[] split = rcTaskVO3List.get(0).split("\t");    
		String s = split[split.length - 1];
		                    
		String[] s2 = s.split("&");     
		
		List l = new ArrayList();
		for(int i = 0; i < s2[0].length(); i ++) {
			l.add(s2[1].charAt(i));
		}
		return l.contains(pf.charAt(0));
	}

}
