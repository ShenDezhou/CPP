package com.jessie.knowledge.taskservice.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class Task extends BaseObject implements Serializable {
	private static final long serialVersionUID = 3832626162173359411L;
	private Integer taskId;
	private String seeds;
	private List<TaskParser> parser;
	private String regex;
	private String listregex;
	private Integer depth;
	private String description;
	private String mark;
	private Integer listcycle;
	private Integer cycle;
	private Double spiderFreq;
	private Integer pageLoadMaxWait;
	//private Integer tryCount;
	//private Double tryInteval;
	private String creator;
	private Boolean loadImg;
	private Timestamp lastupdate;
	private Boolean taskLocked;
	private Integer bufferCount;

	public Integer getBufferCount() {
		return bufferCount;
	}

	public void setBufferCount(Integer bufferCount) {
		this.bufferCount = bufferCount;
	}

	public Task() {

	}

	public List<TaskParser> getParser() {
		return parser;
	}

	public void setParser(List<TaskParser> parser) {
		this.parser = parser;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public String getListregex() {
		return listregex;
	}

	public void setListregex(String listregex) {
		this.listregex = listregex;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getDepth() {
		return depth;
	}

	public void setDepth(Integer depth) {
		this.depth = depth;
	}

	public Double getSpiderFreq() {
		return spiderFreq;
	}

	public void setSpiderFreq(Double spiderFreq) {
		this.spiderFreq = spiderFreq;
	}

	public Integer getPageLoadMaxWait() {
		return pageLoadMaxWait;
	}

	public void setPageLoadMaxWait(Integer pageLoadMaxWait) {
		this.pageLoadMaxWait = pageLoadMaxWait;
	}
/*
	public Double getTryInteval() {
		return tryInteval;
	}

	public void setTryInteval(Double tryInteval) {
		this.tryInteval = tryInteval;
	}
*/
	public Timestamp getLastupdate() {
		return lastupdate;
	}

	public void setLastupdate(Timestamp lastupdate) {
		this.lastupdate = lastupdate;
	}

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

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

	public Boolean getTaskLocked() {
		return taskLocked;
	}

	public void setTaskLocked(Boolean taskLocked) {
		this.taskLocked = taskLocked;
	}
/*
	public Integer getTryCount() {
		return tryCount;
	}

	public void setTryCount(Integer tryCount) {
		this.tryCount = tryCount;
	}
*/
	public Integer getTaskId() {
		return taskId;
	}

	public void setTaskId(Integer taskId) {
		this.taskId = taskId;
	}

	public String getMark() {
		return mark;
	}

	public void setMark(String mark) {
		this.mark = mark;
	}

	public Integer getCycle() {
		return cycle;
	}

	public void setCycle(Integer cycle) {
		this.cycle = cycle;
	}

	public String getSeeds() {
		return seeds;
	}

	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}

	public Boolean getLoadImg() {
		return loadImg;
	}

	public void setLoadImg(Boolean loadImg) {
		this.loadImg = loadImg;
	}

	public Integer getListcycle() {
		return listcycle;
	}

	public void setListcycle(Integer listcycle) {
		this.listcycle = listcycle;
	}
}
