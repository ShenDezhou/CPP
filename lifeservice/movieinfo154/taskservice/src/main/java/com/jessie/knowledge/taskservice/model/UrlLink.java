package com.jessie.knowledge.taskservice.model;

import java.io.Serializable;
import java.util.List;

public class UrlLink extends BaseObject implements Serializable {
	private static final long serialVersionUID = -7403946469908363525L;
	private String url;
	private List<String> linkList;
	
	public UrlLink() {
		
	}
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public List<String> getLinkList() {
		return linkList;
	}
	public void setLinkList(List<String> linkList) {
		this.linkList = linkList;
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
}
