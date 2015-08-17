package net.digitcube.hadoop.model.channel;

import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerWeekInfo;

/**
 * 
 * 注意：
 * 历史原因，在 g.dataeye.com 版本中，周的开始和结束为周一和周日
 * 
 * 原版：
 * 在 g.dataeye.com 版本的周滚存中
 * 玩家的 firstLoginWeekDate/firstPayWeekDate 是用一周的最后一天表示(即周日)
 * 因为 g.dataeye.com 版本数据库表中，周时间的表示字段为：weekId,beginDate,endDate
 *
 * 更改：
 * 在渠道版的周滚存中，
 * firstLoginWeekDate/firstPayWeekDate 改为用一周的开始一天表示(即周一)
 * 因为在版本数据库表中，周时间的表示字段为：weekBeginDate（本周的第一天）
 * 
 * 这样在周游戏行为或周留存时，直接把 weekBeginDate 输出入库即可
 */
public class UserInfoWeekRolling2 {
	private static final String COUNTRY_PROVINCE_SEP = "_C_P_";
	
	private String appId;
	private String appVer;
	private String platform;
	private String uid;
	private String infoBase64;
	private PlayerWeekInfo playerWeekInfo;

	private Date scheduleTime = null;
	
	/**
	 * 构造函数中增加调度时间的原因见: @UserInfoRollingLog 中说明
	 * @param scheduleTime
	 */
	public UserInfoWeekRolling2(Date scheduleTime) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime; 
	};

	public UserInfoWeekRolling2(Date scheduleTime, String[] args) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
		
		int i = 0;
		this.appId = args[i++];
		this.appVer = args[i++];
		this.platform = args[i++];
		this.uid = args[i++];
		this.infoBase64 = args[i++];
	}

	public String[] toStringArray() {
		//this.infoBase64 = PlayInfoUtil.playWeek2Base64(playerWeekInfo);
		this.infoBase64 = PlayInfoUtil.playWeek2Base64(playerWeekInfo, scheduleTime);
		return new String[] { appId, appVer, platform, uid, infoBase64 };
	}

	public PlayerWeekInfo getPlayerWeekInfo() {
		if (playerWeekInfo == null) {
			if (infoBase64 != null) {
				//playerWeekInfo = PlayInfoUtil.playWeekInfoFromStr(infoBase64);
				playerWeekInfo = PlayInfoUtil.playWeekInfoFromStr(infoBase64, scheduleTime);
				
				if (playerWeekInfo == null) {
					playerWeekInfo = new PlayerWeekInfo();
				}
			} else {
				playerWeekInfo = new PlayerWeekInfo();
			}
		}
		return playerWeekInfo;
	}

	public void setPlayerWeekInfo(PlayerWeekInfo playerWeekInfo) {
		this.playerWeekInfo = playerWeekInfo;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAppVer() {
		return appVer;
	}

	public void setAppVer(String appVer) {
		this.appVer = appVer;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getInfoBase64() {
		return infoBase64;
	}

	public void setInfoBase64(String infoBase64) {
		this.infoBase64 = infoBase64;
	}

	public void setCountryAndProvince(String country, String province){
		String areaInfo = country + COUNTRY_PROVINCE_SEP + province;
		this.getPlayerWeekInfo().setGameRegion(areaInfo);
	}
	public String getCountry(){
		return this.getPlayerWeekInfo().getGameRegion().split(COUNTRY_PROVINCE_SEP)[0];
	}
	public String getProvince(){
		return this.getPlayerWeekInfo().getGameRegion().split(COUNTRY_PROVINCE_SEP)[1];
	}
	
	public void markLogin(boolean isLogin) {
		getPlayerWeekInfo().setTrack(getPlayerWeekInfo().getTrack() << 1
				| (isLogin ? 1 : 0));
	}

	public boolean isLogin(int targetDate, int statDate) {
		// 与最新结算日期的间隔
		int weeks = (statDate - targetDate) / (7 * 24 * 3600);
		if (weeks < 0 || weeks > 31) // 超过记录的范围 直接返回false
			return false;
		return (getPlayerWeekInfo().getTrack() >> (weeks) & 1) > 0;
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
		int track = getPlayerWeekInfo().getTrack() << (32 - everDaysTrack);
		return track != 0;
	}
	
	public void markPay(boolean isPay) {
		getPlayerWeekInfo().setPayTrack(getPlayerWeekInfo().getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某周是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int days = (statDate - targetDate) / (7 * 24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (getPlayerWeekInfo().getPayTrack() >> (days) & 1) > 0;
	}

	@Override
	public String toString() {
		return "UserInfoWeekRolling2 [appId=" + appId + ", appVer=" + appVer
				+ ", platform=" + platform + ", uid=" + uid + ", " 
				+ "channel="+getPlayerWeekInfo().getChannel()+ ", "
				+ "gameRegion="+getPlayerWeekInfo().getGameRegion()+ ", "
				+ "track="+getPlayerWeekInfo().getTrack()+ ", "
				+ "payTrack="+getPlayerWeekInfo().getPayTrack()+ ", "
				+"firstLoginWeekDate=" + getPlayerWeekInfo().getFirstLoginWeekDate() + ", " 
				+ "lastLoginWeekDate=" + getPlayerWeekInfo().getLastLoginWeekDate() + ", "
				+ "firstPayWeekDate=" + getPlayerWeekInfo().getFirstPayWeekDate() + ", "
				+ "lastPayWeekDate=" + getPlayerWeekInfo().getLastPayWeekDate() + ", "
				+ "]";
	}
}
