package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * 去重后的在线，按天计算
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:26:38 @copyrigt www.digitcube.net <br>
 * <br>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution ,opersystem,brand,nettype,country,province,operators
 *          ,CurrencyAmount(当日总付费),TotalPayTimes(当日付费次数)
 */

public class OnlineDayLog implements MapReduceVO {
	private String appID;
	private String platform;
	private String accountID;
	private CommonExtend extend;

	// Added at 20140731, 注意：
	// 目前该字段只在统计玩家两次登录时间间隔统计时使用
	// a) 在 OnlineDay MR 输出时，该字段保存的是当天的最后登录时间
	// b) 在滚出输出时，该字段保存的是出当天外历史中最后一次最后登录时间
	private int lastLoginTime;// (最后登陆时间)
	private int totalOnlineTime;// (总在线时长)
	private int maxLevel;// (最高等级)
	private int totalLoginTimes;// (登陆总次数)

	// 玩家当天在线记录，包括登录时间以及该次登录的在线时间，如：
	// {loginTime1:onlineTime1,loginTime2:onlineTime2,...loginTimeX:onlineTimeX}
	private String onlineRecords = "";

	private String uid = "";
	
	private String simCradOp = "";
	
	public final static int INDEX_AppID = 0;
	public final static int INDEX_Platform = 1;
	public final static int INDEX_AccountID = 2;
	public final static int INDEX_LastLoginTime = 15;
	public final static int INDEX_TotalOnlineTime = 16;
	public final static int INDEX_MaxLevel = 17;
	public final static int INDEX_TotalLoginTimes = 18;

	public final static int INDEX_onlineRecords = INDEX_TotalLoginTimes + 1;
	public final static int INDEX_UID = INDEX_onlineRecords + 1;
	//20141025 加入 SIM 卡运营商
	public final static int INDEX_SIM_CAR = INDEX_UID + 1;
	
	public OnlineDayLog(String[] args) {
		appID = args[INDEX_AppID];
		platform = args[INDEX_Platform];
		accountID = args[INDEX_AccountID];
		extend = new CommonExtend(args, -2);// 向前偏移两位
		lastLoginTime = StringUtil.convertInt(args[INDEX_LastLoginTime], 0);
		totalOnlineTime = StringUtil.convertInt(args[INDEX_TotalOnlineTime], 0);
		maxLevel = StringUtil.convertInt(args[INDEX_MaxLevel], 0);
		totalLoginTimes = StringUtil.convertInt(args[INDEX_TotalLoginTimes], 0);

		// onlineRecords是新增属性，不同地方构造 OnlineDayLog 时可能不存在，这里需兼容
		if ((INDEX_onlineRecords + 1) <= args.length) {
			onlineRecords = args[INDEX_onlineRecords];
		}
		if ((INDEX_UID + 1) <= args.length) {
			uid = args[INDEX_UID];
		}
		if((INDEX_SIM_CAR + 1) <= args.length){
			simCradOp = args[INDEX_SIM_CAR];
		}
	}

	public OnlineDayLog() {
	}

	@Override
	public String[] toStringArray() {
		String[] extendArray = extend.toStringArray();
		// String[] array = new String[extendArray.length + 7];
		// String[] array = new String[extendArray.length + 8];
		// String[] array = new String[extendArray.length + 9];
		String[] array = new String[extendArray.length + 10];
		array[INDEX_AppID] = appID;
		array[INDEX_Platform] = platform;
		array[INDEX_AccountID] = accountID;
		System.arraycopy(extendArray, 0, array, 3, extendArray.length);
		array[INDEX_LastLoginTime] = lastLoginTime + "";
		array[INDEX_TotalOnlineTime] = totalOnlineTime + "";
		array[INDEX_MaxLevel] = maxLevel + "";
		array[INDEX_TotalLoginTimes] = totalLoginTimes + "";

		// 玩家在线记录，登录以及该次登录的在线时间
		array[INDEX_onlineRecords] = onlineRecords;
		// 加入uid
		array[INDEX_UID] = uid;
		// 20141025 加入 sim 卡
		array[INDEX_SIM_CAR] = simCradOp;
		
		return array;
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appid) {
		this.appID = appid;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public CommonExtend getExtend() {
		return extend;
	}

	public void setExtend(CommonExtend extend) {
		this.extend = extend;
	}

	/**
	 * Added at 20140731, 注意： 目前该字段只在统计玩家两次登录时间间隔统计时使用 a) 在 OnlineDay MR
	 * 输出时，该字段保存的是当天的最后登录时间 b) 在滚出输出时，该字段保存的是出当天外历史中最后一次最后登录时间
	 * 
	 * @return
	 */
	public int getLastLoginTime() {
		return lastLoginTime;
	}

	public void setLastLoginTime(int lastLoginTime) {
		this.lastLoginTime = lastLoginTime;
	}

	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}

	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}

	public int getMaxLevel() {
		return maxLevel;
	}

	public void setMaxLevel(int maxLevel) {
		this.maxLevel = maxLevel;
	}

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public String getOnlineRecords() {
		return onlineRecords;
	}

	public void setOnlineRecords(String onlineRecords) {
		this.onlineRecords = onlineRecords;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getSimCradOp() {
		return simCradOp;
	}

	public void setSimCradOp(String simCradOp) {
		this.simCradOp = simCradOp;
	}

	@Override
	public String toString() {
		return "OnlineDayLog [appid=" + appID + ", platform=" + platform
				+ ", accountID=" + accountID + ", extend=" + extend
				+ ", lastLoginTime=" + lastLoginTime + ", totalOnlineTime="
				+ totalOnlineTime + ", maxLevel=" + maxLevel
				+ ", totalLoginTimes=" + totalLoginTimes + "]";
	}

}
