package com.jessie.knowledge.taskservice.util;

import java.util.Calendar;

public class DateUtil {
	public static String mill2Date(Long millisecond) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millisecond);
		return calendar.getTime().toString();
		
		//System.out.println(calendar.getTime());
		//DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		//System.out.println(now + " = " + formatter.format(calendar.getTime()));
	}
	
	
	public static void main(String[] args) {
		Long now = System.currentTimeMillis();
		System.out.println(now + "\t" + DateUtil.mill2Date(now));
	}
}
