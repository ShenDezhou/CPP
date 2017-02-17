package com.jessie.knowledge.taskservice.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.jessie.knowledge.taskservice.util.DateUtil;

public class TaskStatus {

	private DB taskDB = null;
	private int maxDepth = 0;
	private Pattern regexPattern = null;
	private Pattern listRegexPattern = null;
	
	
	private BlockingQueue<String> unfetchedUrlQueue = null;
	private HTreeMap<String, Integer> urlDepthMap = null;
	private HTreeMap<String, Long> fetchedUrlMap = null;
	private HTreeMap<String, Integer> failFetchedUrlCountMap = null;
	private Set<String> failFetchedUrlSet = null;
	private HTreeMap<String, Long> fetchingUrlMap = null;
	private Set<String> seedSet = null;
	private Set<String> listUrlSet = null;
	private Set<String> goalUrlSet = null;
	
	private int changeCount = 0;
	private long lastCommitTime = 0;
	
	
	public int getChangeCount() {
		return changeCount;
	}

	public void setChangeCount(int changeCount) {
		this.changeCount = changeCount;
	}
	
	public long getLastCommitTime() {
		return lastCommitTime;
	}

	public void setLastCommitTime(long lastCommitTime) {
		this.lastCommitTime = lastCommitTime;
	}

	public DB getTaskDB() {
		return taskDB;
	}

	public void setTaskDB(DB taskDB) {
		this.taskDB = taskDB;
	}

	public int getMaxDepth() {
		return maxDepth;
	}

	public void setMaxDepth(int maxDepth) {
		this.maxDepth = maxDepth;
	}

	public Pattern getRegexPattern() {
		return regexPattern;
	}

	public void setRegexPattern(Pattern regexPattern) {
		this.regexPattern = regexPattern;
	}

	public Pattern getListRegexPattern() {
		return listRegexPattern;
	}

	public void setListRegexPattern(Pattern listRegexPattern) {
		this.listRegexPattern = listRegexPattern;
	}

	public HTreeMap<String, Integer> getUrlDepthMap() {
		if (null == urlDepthMap) {
			urlDepthMap = taskDB.createHashMap(Constant.DEPTH_MAP)
					.keySerializer(Serializer.STRING).makeOrGet();
		}
		return urlDepthMap;
	}

	public HTreeMap<String, Long> getFetchedUrlMap() {
		if (null == fetchedUrlMap) {
			fetchedUrlMap = taskDB.createHashMap(Constant.FETCHED_URL_MAP)
					.keySerializer(Serializer.STRING).makeOrGet();
		}
		return fetchedUrlMap;
	}
	
	public HTreeMap<String, Integer> getFailFetchedUrlCountMap() {
		if (null == failFetchedUrlCountMap) {
			failFetchedUrlCountMap = taskDB.createHashMap(Constant.FAIL_FETCHED_URL_COUNT_MAP)
					.keySerializer(Serializer.STRING).makeOrGet();
		}
		return failFetchedUrlCountMap;
	}
	
	public Set<String> getFailFetchedUrlSet() {
		if (null == failFetchedUrlSet) {
			failFetchedUrlSet = taskDB.createHashSet(Constant.FAIL_FETCHED_URL_SET)
					.serializer(Serializer.STRING).makeOrGet();
		}
		return failFetchedUrlSet;
	}

	public HTreeMap<String, Long> getFetchingUrlMap() {
		if (null == fetchingUrlMap) {
			fetchingUrlMap = taskDB.createHashMap(Constant.FETCHING_URL_MAP)
					.keySerializer(Serializer.STRING).makeOrGet();;
		}
		return fetchingUrlMap;
	}
	
	public Set<String> getSeedSet() {
		if (null == seedSet) {
			seedSet = taskDB.createHashSet(Constant.SEED_SET)
					.serializer(Serializer.STRING).makeOrGet();
		}
		return seedSet;
	}
	
	public BlockingQueue<String> getUnfetchedUrlQueue() {
		if (null == unfetchedUrlQueue) {
				unfetchedUrlQueue = taskDB.getQueue(Constant.UNFETCHED_URL_QUEUE);
		}
		return unfetchedUrlQueue;
	}
	
	public Set<String> getListUrlSet() {
		if (null == listUrlSet) {
			//taskDB.getTreeSet(Constant.LIST_URL_SET);
			listUrlSet = taskDB.createHashSet(Constant.LIST_URL_SET)
					.serializer(Serializer.STRING).makeOrGet();
		}
		return listUrlSet;
	}

	public Set<String> getGoalUrlSet() {
		if (null == goalUrlSet) {
			goalUrlSet = taskDB.createHashSet(Constant.GOAL_URL_SET)
					.serializer(Serializer.STRING).makeOrGet();
		}
		return goalUrlSet;
	}

	
	public String baseInfo() {
		return toString(100);
	}
	
	public String detailInfo() {
		return toString(100000000);
	}
	
	
	private String toString(int displayCount) {
		String info = "\ntask info:\n";
		
		info += ("\tmax depth: " + maxDepth + "\n");
		if (regexPattern != null) {
			info += ("\turl regex: " + regexPattern.pattern() + "\n");
		}
		if (listRegexPattern != null) {
			info += ("\tlist depth: " + listRegexPattern.pattern() + "\n");
		}
		String urlDepthInfo = "\turl depth : \n";
		for(String key : getUrlDepthMap().keySet()) {
			urlDepthInfo += ("\t\t" + key + " : " + getUrlDepthMap().get(key) + "\n");
		}
		
		String spidedUrls = "\tfetched url size: " + getFetchedUrlMap().keySet().size() + "\n";
		int count = 0;
		List<String> list = new ArrayList<String>();
		for (String key : getFetchedUrlMap().keySet()) {
			if (++count <= displayCount) {
				list.add(key + " : " + DateUtil.mill2Date(Long.valueOf(getFetchedUrlMap().get(key))));
			}
		}
		spidedUrls += ('\t' + list.toString());
		if (count == displayCount) {
			spidedUrls += "...";
		}
		
		list.clear();
		count = 0;
		String spidingUrls = "\tfetching url size: " + getFetchingUrlMap().keySet().size() + "\n";
		for (String key : getFetchingUrlMap().keySet()) {
			if (++count <= displayCount) {
				list.add(key + " : " + DateUtil.mill2Date(Long.valueOf(getFetchingUrlMap().get(key))));
			}
		}
		spidingUrls += ("\t" + list.toString());
		if (count == displayCount) {
			spidingUrls += "...";
		}
		
		
		String seeds = "";
		List<String> itemList = new ArrayList<String>();
		while(!getUnfetchedUrlQueue().isEmpty()) {
			String tempItem = getUnfetchedUrlQueue().poll();
			itemList.add(tempItem);
			if (itemList.size() < displayCount) {
				seeds += (tempItem + ", ");
			} else if (itemList.size() == displayCount) {
				seeds += ("......");
			}
		}
		String seedQueueInfo = "\tseed queue size: " + itemList.size();
		for(String item : itemList) {
			getUnfetchedUrlQueue().add(item);
		}
		seedQueueInfo += seeds;
		
		// 
		
		
		
		return info + spidedUrls + "\n" + spidingUrls + "\n" + seedQueueInfo + "\n";	
		//return info + spidedUrls + "\n" + spidingUrls + "\n" + seedQueueInfo + "\n" + urlDepthInfo;	
	}
	
}
