package com.jessie.knowledge.taskservice.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.springframework.beans.factory.annotation.Value;

import com.jessie.knowledge.taskservice.model.ClientFetchStatus;
import com.jessie.knowledge.taskservice.model.Constant;
import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskStatus;
import com.jessie.knowledge.taskservice.service.UrlService;
import com.jessie.knowledge.taskservice.util.ContainerUtil;
import com.jessie.knowledge.taskservice.util.DateUtil;
import com.jessie.knowledge.taskservice.util.FileUtil;

public class UrlServiceImpl implements UrlService {
	@Value("${dbPath}")
	private String dbPath;
	
	// 遇到list页面是否将depth归零
	@Value("${resetListDepth}")
	private String resetListDepth;
	
	private static final ConcurrentHashMap<Integer, TaskStatus> taskStatusMap = new ConcurrentHashMap<Integer, TaskStatus>();
	private static final ConcurrentHashMap<String, ClientFetchStatus> clientFetchStatusMap = new ConcurrentHashMap<String, ClientFetchStatus>();
	private final Logger logger = Logger.getLogger(getClass());
	
	
	@Override
	public List<String> getUrlList(Task task, int count) {
		// TODO Auto-generated method stub
		
		if (null == task) {
			logger.error("task is null!");
			return null;
		}
		
		ensureTaskExist(task);
		
		int urlCnt = 0;
		List<String> urlList = new ArrayList<String>();
		// recall url when seed queue is empty
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		if (taskStatus.getUnfetchedUrlQueue().isEmpty()) {
			logger.info("task-" + task.getTaskId() + "'s seed queue is empty!");
			recallUrls(task, taskStatus);
		}
		
		Long now = System.currentTimeMillis();
		while (urlCnt++ < count && !taskStatus.getUnfetchedUrlQueue().isEmpty()) {
			String nextUrl = taskStatus.getUnfetchedUrlQueue().poll();
			if (!isUrlExpire(task, taskStatus, nextUrl)) {
				continue;
			}
			urlList.add(nextUrl);
			taskStatus.getFetchingUrlMap().put(nextUrl, now);
		}
		commit(task, true);
		
		logger.info("task-" + task.getTaskId() + " get url list: " + urlList);

		return urlList;
	}

	@Override
	public boolean putUrlList(Task task, String parentUrl, List<String> urlList) {
		// TODO Auto-generated method stub
		if (null == task) {
			logger.error("task is null!");
			return false;
		}
		ensureTaskExist(task);
		
		urlList = ContainerUtil.uniqList(urlList);
		
		logger.info("parent url : " + parentUrl);
		logger.info("link list: " + "   : " + urlList);

		long start = System.currentTimeMillis();
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		
		if (taskStatus.getFetchingUrlMap().keySet().contains(parentUrl)) {
			insertFetchedUrl(taskStatus, parentUrl);
		}
		
		int parentUrlDepth = 0;
		if (taskStatus.getUrlDepthMap().containsKey(parentUrl)) {
			parentUrlDepth = taskStatus.getUrlDepthMap().get(parentUrl);
		}
		
		for(String link : urlList) {
			// unfetched or expire can be inserted
			if (isUrlExpire(task, taskStatus, link)) {
				//System.out.println("insert " + link);
				insertUnfetchedUrl(taskStatus, link, parentUrlDepth+1);
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("insert time: " + (end - start));
		
		commit(task, true);
		long end2 = System.currentTimeMillis();
		System.out.println("commit time: " + (end2 - end));
		return true;
	}


	@Override
	public void updateClientFetchStatus(String host, Task task, String opt, int count) {
		// TODO Auto-generated method stub
		String clientKey = getClientKey(host, task);
		if (!clientFetchStatusMap.containsKey(clientKey)) {
			ClientFetchStatus clientFetchStatus = new ClientFetchStatus();
			clientFetchStatusMap.put(clientKey, clientFetchStatus);
		}
		
		ClientFetchStatus clientFetchStatus = clientFetchStatusMap.get(clientKey);
		
		if (opt.equals(Constant.OPT_FETCH_GET)) {
			clientFetchStatus.setGetUrlCount(clientFetchStatus.getGetUrlCount() + 1);
		} else if (opt.equals(Constant.OPT_FETCH_PUT)) {
			clientFetchStatus.setPutUrlCount(clientFetchStatus.getPutUrlCount() + 1);
		}
		return;
	}

	@Override
	public boolean addSeedList(Task task, List<String> seedList, int depth) {
		// TODO Auto-generated method stub
		if (null == task || depth < 0) {
			return false;
		}
		ensureTaskExist(task);
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		Set<String> seedSet = taskStatus.getSeedSet();
		for(String seedUrl : seedList) {
			seedSet.add(seedUrl);
			insertUnfetchedUrl(taskStatus, seedUrl, depth);
		}
		commit(task, true);
		return true;
	}
	
	@Override
	public void create(Task task, boolean override) {
		// TODO Auto-generated method stub
		if (null == task) {
			return;
		}
		if (override) {
			logger.info("override mode, delete old task if exist!");
			delete(task);
		}
		
		if (!taskStatusMap.containsKey(task.getTaskId())) {
			TaskStatus taskStatus = createTaskStatus(task);
			taskStatusMap.put(task.getTaskId(), taskStatus);
			logger.info("create task-" + task.getTaskId() + " success");
		}
		// insert seed list when task is created
		insertSeed(task);
		commit(task, true);
	}

	@Override
	public void delete(Task task) {
		// TODO Auto-generated method stub
		if (null == task) {
			return;
		}
		
		if (taskStatusMap != null && taskStatusMap.containsKey(task.getTaskId())) {
			logger.info("remove task-" + task.getTaskId() + " status object from memory");
			taskStatusMap.remove(task.getTaskId());
		}
		String curDBPath = getDBPath(task.getTaskId());
		FileUtil.deleteFile(curDBPath);
		logger.info("remove task-" + task.getTaskId() + " from disk");
	}

	@Override
	public void update(Task task) {
		// TODO Auto-generated method stub
		ensureTaskExist(task);
		// suppose maxDepth, regex seed is rewritable
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		initialTaskStatus(task, taskStatus);
		insertSeed(task);
		commit(task, true);
	}
	
	@Override
	public String getTaskStatusInfo(Task task, boolean isDetail) {	
		if (task == null) {
			return "no task!";
		}
		ensureTaskExist(task);
		
		if (null == task || null == taskStatusMap) {
			return Constant.ERROR_TASK_INFO;
		}
		
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		if (null == taskStatus) {
			return Constant.ERROR_TASK_INFO;
		}
		if (isDetail) {
			return taskStatus.detailInfo();
		}
		return taskStatus.baseInfo();
	}
	
	
	private String getClientKey(String host, Task task) {
		return host + task.getTaskId();
	}
	
	private boolean isUrlExpire(Task task, TaskStatus taskStatus, String url) {
		if (null == task || null == taskStatus) {
			return false;
		}

		// check whether list/goal url is expire 
		if (taskStatus.getFetchedUrlMap().containsKey(url)) {
			Long lastFetchTime = taskStatus.getFetchedUrlMap().get(url);
			Long cycle = task.getListcycle() * 1000L;
			
			if (isUrlMatch(taskStatus.getRegexPattern(), url)) {
				cycle = task.getCycle() * 1000L;
			}
			Long now = System.currentTimeMillis();
			if (now - lastFetchTime < cycle) {
				//logger.info(url + " now:" + now + " lastFetchTime:" + lastFetchTime + " " + (now - lastFetchTime) + " cycle:" + cycle);
				return false;
			}
		}

		return true;
	}
	
	
	// recall urls whose fetch time is expired
	private int recallExpiredUrl(TaskStatus taskStatus, Set<String> urlSet, Long cycle) {
		List<String> expiredUrlList = new ArrayList<String>();
		Long now = System.currentTimeMillis();
		
		for (String url : urlSet) {
			if (taskStatus.getFetchingUrlMap().containsKey(url)) {
				continue;
			}
			
			if (taskStatus.getFailFetchedUrlSet().contains(url)) {
				// solution 1: drop it 
				//logger.warn(url + " has failed many times, drop it");
				
				// solution 2: recall
				taskStatus.getFailFetchedUrlSet().remove(url);
				expiredUrlList.add(url);
				continue;
			}
			
			if (!taskStatus.getFetchedUrlMap().containsKey(url)) {
				expiredUrlList.add(url);
			} else {
				Long lastFetchTime = taskStatus.getFetchedUrlMap().get(url);
				if ((now - lastFetchTime) > cycle) {
					expiredUrlList.add(url);
					logger.info("now: " + DateUtil.mill2Date(now) + " lastFetchTime:" + DateUtil.mill2Date(lastFetchTime) + " cycle:" + cycle);	
				}
			}
		}
		for (String url : expiredUrlList) {
			taskStatus.getUnfetchedUrlQueue().add(url);
		}
		
		logger.info("caclExpiredUrl size: " + expiredUrlList.size());
		return expiredUrlList.size();
	}
	
	
	private int recallFailFetchedUrl(TaskStatus taskStatus) {
		int recallCount = 0;
		if (null == taskStatus) {
			logger.error("taskStatus is null!");
		}
		
		HTreeMap<String, Long> fetchingUrlMap = taskStatus.getFetchingUrlMap();
		if (fetchingUrlMap.size() == 0) {
			logger.info("fetchingUrlMap size is 0\t");
			return recallCount;
		}

		Long now = System.currentTimeMillis();
		HTreeMap<String, Integer> failFetchedUrlCountMap = taskStatus.getFailFetchedUrlCountMap();
		Set<String> failFetchedUrlSet = taskStatus.getFailFetchedUrlSet();
		List<String> recallFailFetchUrlList = new ArrayList<String>();
		for (String url : fetchingUrlMap.keySet()) {
			if ((now - fetchingUrlMap.get(url)) > Constant.FAIL_FETCHED_CYCLE) {
				// check whether this url has fail fetch many times or not
				if (!failFetchedUrlCountMap.containsKey(url)) {
					logger.info(url + " is first re-fetch!");
					failFetchedUrlCountMap.put(url, 1);
					recallFailFetchUrlList.add(url);
				} else if (failFetchedUrlCountMap.get(url) < Constant.MAX_FAIL_FETCH_COUNT) {
					logger.info(url + " is " + failFetchedUrlCountMap.get(url) + "th re-fetch!");
					failFetchedUrlCountMap.put(url, failFetchedUrlCountMap.get(url) + 1);
					recallFailFetchUrlList.add(url);
				} else {
					// fail MAX_FAIL_FETCH_COUNT times
					failFetchedUrlCountMap.remove(url);
					failFetchedUrlSet.add(url);
				}
			}
		}
		
		for (String url : recallFailFetchUrlList) {
			taskStatus.getFetchingUrlMap().remove(url);
			taskStatus.getUnfetchedUrlQueue().add(url);
		}
		
		//FAIL_FETCH_CYCLE
		recallCount = recallFailFetchUrlList.size();
		return recallCount;
	}
	
	private void recallUrls(Task task, TaskStatus taskStatus) {
		logger.info("recall expired url for task-" + task.getTaskId());
		if (null == task || null == taskStatus) {
			return;
		}
		
		// spider client get the url, but it doesn't fetched success
		int recallCount = 0;
		recallCount = recallFailFetchedUrl(taskStatus);
		if (recallCount > 0) {
			logger.info("recall fail fetch url done!");
			return;
		}
		
		// get expired seed/list url
		Long listUrlCycle = task.getListcycle() * 1000L;
		//recallCount = recallExpiredUrl(taskStatus, taskStatus.getListUrlSet(), listUrlCycle);
		recallCount = recallExpiredUrl(taskStatus, taskStatus.getSeedSet(), listUrlCycle);
		
		
		logger.debug("task-" + task.getTaskId() +  " 's listUrlCycle :" + listUrlCycle);
		logger.debug("list url set:" + taskStatus.getListUrlSet().size());
		logger.debug("recall list url count:" + recallCount);
	}
	
	
	private TaskStatus createTaskStatus(Task task) {
		TaskStatus taskStatus = new TaskStatus();
		initialTaskStatus(task, taskStatus);
		DB db = createOrGetDB(task.getTaskId());
		taskStatus.setTaskDB(db);
		return taskStatus;
	}
	
	
	private void initialTaskStatus(Task task, TaskStatus taskStatus) {
		if (null == task || null == taskStatus) {
			logger.error("init taskStatus of task-" + task.getTaskId() + " error!!!");
			return;
		}
		taskStatus.setMaxDepth(task.getDepth());
		taskStatus.setChangeCount(0);
		taskStatus.setLastCommitTime(System.currentTimeMillis());
		String regex = task.getRegex();
		if (!regex.isEmpty()) {
			taskStatus.setRegexPattern(Pattern.compile(regex));
		}
		String listRegex = task.getListregex();
		if (!listRegex.isEmpty()) {
			taskStatus.setListRegexPattern(Pattern.compile(listRegex));
		}
		logger.info("init taskStatus of task-" + task.getTaskId() + " success");
	}
	
	// put fetched url into fetchedUrlMap
	private void insertFetchedUrl(TaskStatus taskStatus, String url) {
		if (null == taskStatus) {
			logger.error("taskStatus is null, insert fetched url [" + url + "] error");
			return;
		}
		long now = System.currentTimeMillis();
		taskStatus.getFetchedUrlMap().put(url, now);

		taskStatus.getFetchingUrlMap().remove(url);
		taskStatus.getFailFetchedUrlCountMap().remove(url);
	}
	
	
	private String getDBName(Integer taskid) {
		return "task_" + taskid + ".db";
	}
	
	
	private String getDBPath(Integer taskid) {
		File dbDir = new File(dbPath);
		if (!dbDir.exists()) {
			dbDir.mkdirs();
		}
		return dbPath + File.separator + getDBName(taskid);
	}
	
	
	private DB createOrGetDB(Integer taskid) {
		String curDBPath = getDBPath(taskid);
		return DBMaker.newFileDB(new File(curDBPath))
				.transactionDisable()
				//.cacheSize(Constant.CACHE_SIZE)
				.mmapFileEnable()
				.closeOnJvmShutdown().make();
	}	

	// load task exist on disk
	private void loadTask(Task task) {
		if (taskStatusMap.containsKey(task.getTaskId())) {
			return;
		}
		TaskStatus taskStatus = createTaskStatus(task);
		taskStatusMap.put(task.getTaskId(), taskStatus);
	}
	
	// whether task exist in memory or not
	private boolean taskExist(Task task) {
		if (null == task || null == taskStatusMap) {			
			return false;
		}
		return taskStatusMap.containsKey(task.getTaskId());
	}
	
	// whether task exist on disk or not
	private boolean taskDBExist(Task task) {
		if (null == task) {
			return false;
		}
		String curDBPath = getDBPath(task.getTaskId());
		return (new File(curDBPath)).exists();
	}
	
	// ensure task will exist, if not exist, create it or load it
	private void ensureTaskExist(Task task) {
		if (taskExist(task)) {
			logger.debug("task is exist! go on");
			return;
		} else if (taskDBExist(task)) {
			logger.info("task is on the disk, load it into memory");
			loadTask(task);
		} else {
			logger.info("task is not exist, create it");
			create(task, true);
		}
	}
	
	
	// insert seeds when task is created, don't check last fetch time of seeds
	private boolean insertSeed(Task task) {
		logger.info("task " + task.getTaskId() + " first insert seed!");
		
		String seedFile = task.getSeeds();
		logger.info("task " + task.getTaskId() + " seed file is :" + seedFile);
		List<String> seedList = FileUtil.readFileToList(seedFile);

		if (null == seedList || seedList.size() == 0) {
			logger.info("seed file " +  seedFile + " is error, or is empty!");
			return false;
		}
		logger.info("task " + task.getTaskId() + " has " + seedList.size() + " seeds");	
		

		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		if (null == taskStatus) {
			logger.error("task " + task.getTaskId() + "'s taskStatus error!");
			for(Integer key : taskStatusMap.keySet()) {
				logger.info("taskStatus: key = " + key + "  taskStatus: " + (taskStatus==null));
			}
			
			return false;
		}
		
		int lineCnt = 0;
		Set<String> seedSet = taskStatus.getSeedSet();
		for (String seedUrl : seedList) {
			seedSet.add(seedUrl);
			boolean insertStatus = insertUnfetchedUrl(taskStatus, seedUrl, Constant.ROOT_DEPTH);
			if (++lineCnt % 5000 == 0) {
				logger.info(task.getTaskId() + " " + lineCnt + "th seed [" + seedUrl + "] insert status: " + insertStatus);
			}
		}
		logger.info("task " + task.getTaskId() + " has " + seedList.size() + " seeds, load done.");
		return true;
	}
	
	
	// insert seed urls(link url) when task is running 
	private boolean insertUnfetchedUrl(TaskStatus taskStatus, String url, int depth) {
		if (depth > taskStatus.getMaxDepth()) {
			logger.error("seed(link) url: [ " + url + " ] depth is " + depth + ", filter it!");
			return false;
		}
		if (taskStatus.getMaxDepth() == Constant.ROOT_DEPTH && !isUrlMatch(taskStatus.getRegexPattern(), url)) {
			logger.error("seed(link) url: [ " + url + " ] is not a goal url!");
			return false;
		}

		if (resetListDepth.equals("true")) {
			// 遇到list页面，归零其深度
			if (isUrlMatch(taskStatus.getListRegexPattern(), url)) {
				depth = Constant.ROOT_DEPTH;
			}
		} else {
			// solution 2 解决电影的更新的情况
	        int maxDepth = taskStatus.getMaxDepth();
	        if (maxDepth > 0 && depth < maxDepth) {
	        	if(taskStatus.getListRegexPattern() != null && !isUrlMatch(taskStatus.getListRegexPattern(), url)) {
	        		return false;
	        	}
	        }
	        if (maxDepth == depth && !isUrlMatch(taskStatus.getRegexPattern(), url)) {
	        	return false;
	        }
		}
		
		if (depth == Constant.ROOT_DEPTH) {
			taskStatus.getListUrlSet().add(url);
		} else if (isUrlMatch(taskStatus.getRegexPattern(), url)) {
			taskStatus.getGoalUrlSet().add(url);
		}
		
		// url's depth is the shortest path to root
		taskStatus.getUnfetchedUrlQueue().add(url);
		if (!taskStatus.getUrlDepthMap().containsKey(url) || 
				taskStatus.getUrlDepthMap().get(url) > depth) {
			taskStatus.getUrlDepthMap().put(url, depth);
		}
		
		logger.debug("put url depth: [" + url + "] : " + depth);
		
		logger.debug("put url [" + url + "] into seed set done!");
		return true;
	}
	
	private boolean isUrlMatch(Pattern pattern, String url) {
		if (null == pattern) {
			return false;
		}
		Matcher matcher = pattern.matcher(url);
		return matcher.find();
	}
	
	// commit task status from memory to disk
	private void commit(Task task, boolean force) {
		if (null == task || null == taskStatusMap) {
			return;
		}
		
		TaskStatus taskStatus = taskStatusMap.get(task.getTaskId());
		DB db = taskStatus.getTaskDB();
		if (db != null ) {
			db.commit();
		}
		
		
		/*
		long current = System.currentTimeMillis();
		if (current - taskStatus.getLastCommitTime() >= Constant.COMMIT_CYCLE) {
			DB db = taskStatus.getTaskDB();
			if (db != null ) {
				db.commit();
				taskStatus.setLastCommitTime(current);
				logger.info(DateUtil.mill2Date(current) + " commit db!!!");
			}
		}
		*/
	}
	
}
