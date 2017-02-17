package com.jessie.knowledge.taskservice.service;

import java.util.List;

import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskSearchResult;

public interface TaskService {
	List<Task> findTaskByPropertyValue(String propertyName, Object value);

	List<Task> findTaskByPropertyRange(String propertyName, Double lowValue,
			Double highValue);

	Task getTaskById(Integer id);

	void createTask(Task t);

	void deleteTask(Integer id);

	void updateTask(Task t);

	TaskSearchResult searchTask(String searchStr, String searchField,
			String sortField, String order, Integer limit, Integer offset);

	TaskSearchResult getUserTask(String username);

}
