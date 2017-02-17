package com.jessie.knowledge.taskservice.model;

public class ClientFetchStatus {
	private int getUrlCount;
	private int putUrlCount;
	private float succFetchRatio;
	
	public ClientFetchStatus() {
		getUrlCount = 0;
		putUrlCount = 0;
		succFetchRatio = 0.0f;
	}

	public int getGetUrlCount() {
		return getUrlCount;
	}

	public void setGetUrlCount(int getUrlCount) {
		this.getUrlCount = getUrlCount;
	}

	public int getPutUrlCount() {
		return putUrlCount;
	}

	public void setPutUrlCount(int putUrlCount) {
		this.putUrlCount = putUrlCount;
	}

	public float getSuccFetchRatio() {
		return succFetchRatio;
	}

	public void setSuccFetchRatio(float succFetchRatio) {
		this.succFetchRatio = succFetchRatio;
	}



}
