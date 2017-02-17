package com.jessie.knowledge.taskservice.service;

import java.util.List;

import com.jessie.knowledge.taskservice.model.UrlLink;
import com.jessie.knowledge.taskservice.model.Task;

public interface UrlService {
	public List<String> getUrlList(Task task, int count);

	public boolean putUrlList(Task task, String parentUrl, List<String> urlList);

	public boolean addSeedList(Task task, List<String> seedList, int depth);
	
	public void create(Task task, boolean override);
	
	public void delete(Task task);
	
	public void update(Task task);
	
	public String getTaskStatusInfo(Task task, boolean isDetail);
	
	public void updateClientFetchStatus(String host, Task task, String opt, int count);
}
