package com.jessie.knowledge.taskservice.util;

import java.util.ArrayList;
import java.util.List;

public class ContainerUtil {

	public static List<String> uniqList(List<String> list) {
		List<String> uList = new ArrayList<String>();
		for (String item : list) {
			if (!uList.contains(item)) {
				uList.add(item);
			}
		}
		return uList;
	}
	
}
