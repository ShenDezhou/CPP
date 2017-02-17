package com.jessie.knowledge.taskservice.model;

public class Constant {

	public static final String DB_FILE_REGEX = "[^_]+_[^_]+.db$";
	
	public final static int ROOT_DEPTH = 0;
	public final static int MAX_FAIL_FETCH_COUNT = 5;
	
	public static final String USER = "user";
	public static final String TASKID = "taskid";
	public static final String REGEX = "regex";
	public static final String LIST_REGEX = "listRegex";
	public static final String MAX_DEPTH = "maxDepth";
	
	public static final String SEED_SET = "seedSet";
	public static final String UNFETCHED_URL_QUEUE = "unFetchedQueue";
	public static final String DEPTH_MAP = "depthMap";
	public static final String FETCHED_URL_MAP = "fetchedUrlMap";
	public static final String FETCHING_URL_MAP = "fetchingUrlMap";
	public static final String FAIL_FETCHED_URL_COUNT_MAP = "failFetchedUrlCountMap";
	public static final String FAIL_FETCHED_URL_SET = "failedFetchedUrlSet";
	public static final String FAIL_FETCHED_URL_MAP = "failFetchUrlMap";
	public static final String GOAL_URL_SET = "goalUrlSet";
	public static final String LIST_URL_SET = "listUrlSet";
	
	public static final String ERROR_TASK_INFO = "invalid task";

	
	// 10 min
	public static final Integer FAIL_FETCHED_CYCLE = 10 * 60 * 1000;
	public static final Integer CACHE_SIZE = 32768 * 100;
	
	// 30 min
	public static final long COMMIT_CYCLE = 30 * 60 * 1000;
	// success fetch ratio threshold
	public static final Float SUCC_FETCH_RATIO = 0.6f;
	
	// option 
	public static final String OPT_TASK_RESET = "reset";
	
	public static final String OPT_FETCH_GET = "get";
	public static final String OPT_FETCH_PUT = "put";
	
	
	
	
}

