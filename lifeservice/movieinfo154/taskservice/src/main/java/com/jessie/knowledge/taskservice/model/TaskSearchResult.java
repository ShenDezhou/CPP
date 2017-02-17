package com.jessie.knowledge.taskservice.model;

import java.util.List;

public class TaskSearchResult {
	private Integer total;
	private List<Task> rows;

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public List<Task> getRows() {
		return rows;
	}

	public void setRows(List<Task> rows) {
		this.rows = rows;
	}
}
