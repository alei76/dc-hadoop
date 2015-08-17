package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.util.StringUtil;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月22日 下午8:40:25 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class OSS implements MapReduceVO {

	private int timestamp;
	private String appID;
	private String platform;
	private String monitorName;
	private String callerDesc1;
	private String cost;
	private String success;
	private String failtimes;
	private final static int Index_Platform = 2;
	private final static int Index_MonitorName = 3;
	private final static int Index_CallerDesc1 = 4;
	private final static int Index_Cost = 5;
	private final static int Index_Success = 6;
	private final static int Index_Failtimes = 7;

	public OSS() {
	}

	public OSS(String[] args) {
		timestamp = StringUtil.convertInt(args[Constants.INDEX_TIMESTAMP], 0);
		appID = args[Constants.INDEX_APPID];
		platform = args[Index_Platform];
		monitorName = args[Index_MonitorName];
		callerDesc1 = args[Index_CallerDesc1];
		cost = args[Index_Cost];
		success = args[Index_Success];
		failtimes = args[Index_Failtimes];
	}

	@Override
	public String[] toStringArray() {

		return new String[] { timestamp + "", appID, platform, monitorName,
				callerDesc1, cost, success, failtimes };
	}

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
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

	public String getMonitorName() {
		return monitorName;
	}

	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}

	public String getCallerDesc1() {
		return callerDesc1;
	}

	public void setCallerDesc1(String callerDesc1) {
		this.callerDesc1 = callerDesc1;
	}

	public String getCost() {
		return cost;
	}

	public void setCost(String cost) {
		this.cost = cost;
	}

	public String getSuccess() {
		return success;
	}

	public void setSuccess(String success) {
		this.success = success;
	}

	public String getFailtimes() {
		return failtimes;
	}

	public void setFailtimes(String failtimes) {
		this.failtimes = failtimes;
	}

}
