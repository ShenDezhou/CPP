package com.jessie.knowledge.taskservice.dao;

import java.util.List;

import com.jessie.knowledge.taskservice.model.UserTask;

public interface UserTaskDao {
	public void create(UserTask transientInstance);

	public void update(UserTask instance);

	public void delete(UserTask persistentInstance);

	public UserTask findById(java.lang.Integer id);

	public List findByProperty(String propertyName, Object value);
}
