package com.jessie.knowledge.taskservice.util;

import java.util.Random;

import org.apache.log4j.Logger;

public class MathUtil {
	private final static Logger logger = Logger.getLogger("MathUtil");
	
	
	public static float random() {
		Random random = new Random(System.currentTimeMillis());		
		return random.nextFloat();
	}
	
	
}
