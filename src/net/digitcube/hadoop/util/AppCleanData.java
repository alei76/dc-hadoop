package net.digitcube.hadoop.util;

import java.util.Set;

public class AppCleanData {

	private String appID;

	private String platform;

	private String deleteTime;

	private Set<String> channels;

	public AppCleanData() {

	}

	public AppCleanData(String appID, String platform, String deleteTime, Set<String> channels) {
		this.appID = appID;
		this.platform = platform;
		this.deleteTime = deleteTime;
		this.channels = channels;
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appID) {
		this.appID = appID;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getDeleteTime() {
		return deleteTime;
	}

	public void setDeleteTime(String deleteTime) {
		this.deleteTime = deleteTime;
	}

	public Set<String> getChannels() {
		return channels;
	}

	public void setChannels(Set<String> channels) {
		this.channels = channels;
	}

	@Override
	public String toString() {
		return "appID:" + appID + ",platform:" + platform + ",deleteTime:" + deleteTime + ",channels:" + channels;
	}
}