package com.jessie.knowledge.taskservice.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.jessie.knowledge.taskservice.dao.TaskDao;
import com.jessie.knowledge.taskservice.dao.UserTaskDao;
import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskSearchResult;
import com.jessie.knowledge.taskservice.model.UserTask;
import com.jessie.knowledge.taskservice.service.TaskService;

public class TaskServiceImpl implements TaskService {
	private final Logger log = Logger.getLogger(getClass());
	private TaskDao taskDao;
	private UserTaskDao userTaskDao;

	// private SeedManageService seedManageService;

	public TaskDao getTaskDao() {
		return taskDao;
	}

	public void setTaskDao(TaskDao taskDao) {
		this.taskDao = taskDao;
	}

	public UserTaskDao getUserTaskDao() {
		return userTaskDao;
	}

	public void setUserTaskDao(UserTaskDao userTaskDao) {
		this.userTaskDao = userTaskDao;
	}


	@Override
	public List<Task> findTaskByPropertyValue(String propertyName, Object value) {
		// TODO Auto-generated method stub
		return taskDao.findByProperty(propertyName, value);

	}

	@Override
	public List<Task> findTaskByPropertyRange(String propertyName,
			Double lowValue, Double highValue) {
		// TODO Auto-generated method stub
		return taskDao.findByPropertyRange(propertyName, lowValue, highValue);
	}

	@Override
	public Task getTaskById(Integer id) {
		// TODO Auto-generated method stub
		return taskDao.findById(id);
	}

	@Override
	public void createTask(Task t) {
		// TODO Auto-generated method stub
		taskDao.create(t);
	}

	@Override
	public void deleteTask(Integer id) {
		// TODO Auto-generated method stub
		taskDao.delete(taskDao.findById(id));
	}

	@Override
	public void updateTask(Task t) {
		// TODO Auto-generated method stub
		taskDao.update(t);
	}

	@Override
	public TaskSearchResult searchTask(String searchStr, String searchField,
			String sortField, String order, Integer limit, Integer offset)

	{
		// TODO Auto-generated method stub
		return taskDao.searchTask(searchStr, searchField, sortField, order,
				limit, offset);
	}

	@Override
	public TaskSearchResult getUserTask(String username) {
		// TODO Auto-generated method stub
		List<UserTask> userTask = userTaskDao.findByProperty("username",
				username);
		TaskSearchResult result = new TaskSearchResult();
		List<Task> taskList = new ArrayList();
		for (UserTask u : userTask) {
			Task t = taskDao.findById(u.getTaskId());
			if (t == null) {
				log.warn("taskid:" + u.getTaskId() + " not found in task");
			} else {
				taskList.add(t);
			}
		}
		result.setRows(taskList);
		result.setTotal(taskList.size());
		return result;
	}
}
