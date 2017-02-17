package com.jessie.knowledge.taskservice.dao;

import java.util.List;

import com.jessie.knowledge.taskservice.model.Task;
import com.jessie.knowledge.taskservice.model.TaskSearchResult;

public interface TaskDao {

	public void create(Task transientInstance);

	public void update(Task instance);

	public void delete(Task persistentInstance);

	public Task findById(java.lang.Integer id);

	public List findByProperty(String propertyName, Object value);

	public List findByPropertyRange(String propertyName, Double lowValue,
			Double highValue);

	public TaskSearchResult searchTask(String searchStr, String searchField,
			String sortField, String order, Integer limit, Integer offset);

}