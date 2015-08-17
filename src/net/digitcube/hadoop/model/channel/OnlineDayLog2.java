package net.digitcube.hadoop.model.channel;

import net.digitcube.hadoop.mapreduce.channel.ExtendFieldsHelper;
import net.digitcube.hadoop.util.StringUtil;

public class OnlineDayLog2 {

	public String appId;
	public String platform;
	public String appVer;
	public String uid;
	public String channel;
	public String extend;
	public int totalLoginTimes = 0;
	public int totalOnlineTime = 0;
	public String onlineRecords = "";
	public String isNewPlayer = "N";
	public ExtendFieldsHelper extendsHelper = null;
	
	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAppVer() {
		return appVer;
	}

	public void setAppVer(String appVer) {
		this.appVer = appVer;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getExtend() {
		return extend;
	}

	public void setExtend(String extend) {
		this.extend = extend;
	}

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}

	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}

	public String getOnlineRecords() {
		return onlineRecords;
	}

	public void setOnlineRecords(String onlineRecords) {
		this.onlineRecords = onlineRecords;
	}

	public String getIsNewPlayer() {
		return isNewPlayer;
	}

	public void setIsNewPlayer(String isNewPlayer) {
		this.isNewPlayer = isNewPlayer;
	}

	public synchronized ExtendFieldsHelper getExtendsHelper() {
		if(null == extendsHelper){
			extendsHelper = new ExtendFieldsHelper(extend);
		}
		return extendsHelper;
	}

	public void setExtendsHelper(ExtendFieldsHelper extendsHelper) {
		this.extendsHelper = extendsHelper;
	}

	public OnlineDayLog2(HeaderLog2 headerLog) {
		this.appId = headerLog.getAppID();
		this.appVer = headerLog.getAppVersion();
		this.platform = headerLog.getPlatform();
		this.channel = headerLog.getChannel();
		this.uid = headerLog.getUID();
		this.extend = headerLog.getExtend();
	}
	
	public OnlineDayLog2(String[] fields) {
		int i = 0;
		this.appId = fields[i++];
		this.appVer = fields[i++];
		this.platform = fields[i++];
		this.channel = fields[i++];
		this.uid = fields[i++];
		this.extend = fields[i++];
		this.totalLoginTimes = StringUtil.convertInt(fields[i++], 0);
		this.totalOnlineTime = StringUtil.convertInt(fields[i++], 0);
		this.onlineRecords = fields[i++];
		this.isNewPlayer = fields[i++];
	}
	
	public String[] toStringArr(){
		return new String[]{
				this.appId,
				this.appVer,
				this.platform,
				this.channel,
				this.uid,
				this.extend,
				this.totalLoginTimes+"",
				this.totalOnlineTime+"",
				this.onlineRecords,
				this.isNewPlayer
		};
	}
}
