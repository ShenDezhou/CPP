package com.jessie.knowledge.taskservice.model;

public class UserTask {
	private Integer id;
	private String username;
	private Integer taskId;
	private Integer runningMode;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Integer getTaskId() {
		return taskId;
	}

	public void setTaskId(Integer taskId) {
		this.taskId = taskId;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getRunningMode() {
		return runningMode;
	}

	public void setRunningMode(Integer runningMode) {
		this.runningMode = runningMode;
	}
}
