package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.util.StringUtil;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:28:32 @copyrigt www.digitcube.net
 */

public class CommonHeader implements MapReduceVO {

	private int timestamp;
	private String appID;
	private String uid;
	private String accountID;
	private String platform;

	public CommonHeader() {
	}

	public CommonHeader(String[] args) {
		timestamp = StringUtil.convertInt(args[Constants.INDEX_TIMESTAMP], 0);
		appID = args[Constants.INDEX_APPID];
		uid = args[Constants.INDEX_UID];
		accountID = args[Constants.INDEX_ACCOUNTID];
		platform = args[Constants.INDEX_PLATFORM];
	}

	public String[] toStringArray() {
		return new String[] { timestamp + "", appID, uid, accountID, platform };
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

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

}
