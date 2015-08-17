package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * UID 信息每日滚存数据
 */
public class UIDRollingLog {
	
	private String appID;
	private String version;
	private String platform;
	private String channel;
	private String gameServer;
	private String uid;
	private String accountID;

	private int firstLoginDate = 0;
	private int lastLoginDate = 0;
	private int totalOnlineDay = 0;
	private int totalLoginTimes = 0;
	private int track = 0;

	public UIDRollingLog(){};
	public UIDRollingLog(String[] args) {
		int i = 0;
		this.appID = args[i++];
		this.version = args[i++];
		this.platform = args[i++];
		this.channel = args[i++];
		this.gameServer = args[i++];
		this.uid = args[i++];
		this.accountID = args[i++];
		this.firstLoginDate = StringUtil.convertInt(args[i++], 0);
		this.lastLoginDate = StringUtil.convertInt(args[i++], 0);
		this.totalOnlineDay = StringUtil.convertInt(args[i++], 0);
		this.totalLoginTimes = StringUtil.convertInt(args[i++], 0);
		this.track = StringUtil.convertInt(args[i++], 0);
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appID) {
		this.appID = appID;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
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

	public int getFirstLoginDate() {
		return firstLoginDate;
	}

	public void setFirstLoginDate(int firstLoginDate) {
		this.firstLoginDate = firstLoginDate;
	}

	public int getLastLoginDate() {
		return lastLoginDate;
	}

	public void setLastLoginDate(int lastLoginDate) {
		this.lastLoginDate = lastLoginDate;
	}

	public int getTotalOnlineDay() {
		return totalOnlineDay;
	}

	public void setTotalOnlineDay(int totalOnlineDay) {
		this.totalOnlineDay = totalOnlineDay;
	}

	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}

	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}

	public int getTrack() {
		return track;
	}

	public void setTrack(int track) {
		this.track = track;
	}

	public String[] toStringArray(){
		return new String[]{
			this.appID,
			this.version,
			this.platform,
			this.channel,
			this.gameServer,
			this.uid,
			this.accountID,
			this.firstLoginDate+"",
			this.lastLoginDate+"",
			this.totalOnlineDay+"",
			this.totalLoginTimes+"",
			this.track+""
		};
	}
	
	/**
	 * @param isLogin
	 */
	public void markLogin(boolean isLogin) {
		this.setTrack(this.getTrack() << 1 | (isLogin ? 1 : 0));
	}

	//
	/**
	 * 判断某天是否登录过
	 * 
	 * @param targetDate
	 *            目标日期，需要判断是否登陆的日期
	 * @param statDate
	 *            最新的统计日期，最后调用markLogin的日期
	 * @return
	 */
	public boolean isLogin(int targetDate, int statDate) {
		// 与最新结算日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (this.getTrack() >> (days) & 1) > 0;
	}

	/**
	 * 从统计日期之前N天内是否有登陆过
	 * 
	 * @param everDaysTrack
	 *            everDaysTrack=7 指包含结算日在内的往过去算7天
	 * @return
	 */
	public boolean isEverLogin(int everDaysTrack) {
		if (everDaysTrack > 32) // 超过记录的范围 直接返回false
			return false;
		int track = this.getTrack() << (32 - everDaysTrack);
		return track != 0;
	}
}
