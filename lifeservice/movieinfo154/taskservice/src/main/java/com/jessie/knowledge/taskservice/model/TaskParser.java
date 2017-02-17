package com.jessie.knowledge.taskservice.model;

import java.io.Serializable;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TaskParser extends BaseObject implements Serializable {

	private static final long serialVersionUID = 1303116567003063616L;
	private Integer id;
	private Task task;
	private String parserUrl;
	private UUID tableId;
	private String creator;
	private Double latestVersion;

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean equals(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getParserUrl() {
		return parserUrl;
	}

	public void setParserUrl(String parserUrl) {
		this.parserUrl = parserUrl;
	}

	public UUID getTableId() {
		return tableId;
	}

	public void setTableId(UUID tableId) {
		this.tableId = tableId;
	}

	public Double getLatestVersion() {
		return latestVersion;
	}

	public void setLatestVersion(Double latestVersion) {
		this.latestVersion = latestVersion;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@JsonIgnore
	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

}
