package com.jessie.knowledge.taskservice.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class FileUtil {	
	private final static Logger logger = Logger.getLogger("FileUtil");
	
	public static boolean deleteFile(String path) {
		File file = new File(path);
		if (!file.exists()) {
			return true;
		}
		if (file.isDirectory()) {
			String[] children = file.list();
			for(int idx=0; idx<children.length; ++idx) {
				boolean delSucc = deleteFile(path + File.separator + children[idx]);
				if (!delSucc) {
					return false;
				}
			}
		}
		return file.delete();
	}
	
	
	private static List<String> readLocalFileToList(String filePath) {
		List<String> list = new ArrayList<String>();
		try {
			String line = "";
			File file = new File(filePath);
			if (!file.exists()) {
				logger.error(filePath + " is not exist!");
				return list;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file))); 
			while ((line = br.readLine()) != null) {
				list.add(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	private static List<String> readRemoteFileToList(String filePath) {
		List<String> list = new ArrayList<String>();
		logger.info("read seed file: " + filePath);
		try {
			String line = "";
			URL url = new URL(filePath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
			while((line = reader.readLine()) != null) {
				list.add(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public static List<String> readFileToList(String filePath) {
		logger.info("begin to read seed file: " + filePath);
		if (filePath.startsWith("http://")) {
			return readRemoteFileToList(filePath);
		}
		return readLocalFileToList(filePath);
	}
	
}
