package net.digitcube.hadoop.model.channel;

import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerMonthInfo;

/**
 * 
 * 注意：
 * 原版：
 * 在 g.dataeye.com 版本的周滚存中
 * 玩家的 firstLoginMonthDate/firstPayMonthDate 是用月的最后一天表示
 * 因为 g.dataeye.com 版本数据库表中，月时间的表示字段为：monthId,beginDate,endDate
 *
 * 更改：
 * 在渠道版的月滚存中，
 * firstLoginMonthDate/firstPayMonthDate 改为用月的第一天表示(即1号)
 * 因为在渠道版数据库表中，月时间的表示字段为：monthBeginDate（本月的第一天）
 * 
 * 这样在月游戏行为或月留存时，直接把 monthBeginDate 输出入库即可
 */
public class UserInfoMonthRolling2{
	private static final String COUNTRY_PROVINCE_SEP = "_C_P_";
	
	private String appId;
	private String appVer;
	private String platform;
	private String uid;
	private String infoBase64;
	private PlayerMonthInfo playerMonthInfo;

	private Date scheduleTime = null;
	
	/**
	 * 构造函数中增加调度时间的原因见: @UserInfoRollingLog 中说明
	 * @param scheduleTime
	 */
	public UserInfoMonthRolling2(Date scheduleTime) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
	};

	public UserInfoMonthRolling2(Date scheduleTime, String[] args) {
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
		//this.infoBase64 = PlayInfoUtil.playMonth2Base64(playerMonthInfo);
		this.infoBase64 = PlayInfoUtil.playMonth2Base64(playerMonthInfo, scheduleTime);
		return new String[] { appId, appVer, platform, uid, infoBase64 };
	}

	public PlayerMonthInfo getPlayerMonthInfo() {
		if (playerMonthInfo == null) {
			if (infoBase64 != null) {
				//playerMonthInfo = PlayInfoUtil.playMonthInfoFromStr(infoBase64);
				playerMonthInfo = PlayInfoUtil.playMonthInfoFromStr(infoBase64, scheduleTime);
				
				if (playerMonthInfo == null) {
					playerMonthInfo = new PlayerMonthInfo();
				}
			} else {
				playerMonthInfo = new PlayerMonthInfo();
			}
		}
		return playerMonthInfo;
	}

	public void setPlayerMonthInfo(PlayerMonthInfo playerMonthInfo) {
		this.playerMonthInfo = playerMonthInfo;
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
		this.getPlayerMonthInfo().setGameRegion(areaInfo);
	}
	public String getCountry(){
		return this.getPlayerMonthInfo().getGameRegion().split(COUNTRY_PROVINCE_SEP)[0];
	}
	public String getProvince(){
		return this.getPlayerMonthInfo().getGameRegion().split(COUNTRY_PROVINCE_SEP)[1];
	}

	public void markLogin(boolean isLogin) {
		getPlayerMonthInfo().setTrack(getPlayerMonthInfo().getTrack() << 1
				| (isLogin ? 1 : 0));
	}

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
		int month = getMonthMinus(statDate, targetDate);
		if (month > 31) // 超过记录的范围 直接返回false
			return false;
		return (getPlayerMonthInfo().getTrack() >> (month) & 1) > 0;
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
		int track = getPlayerMonthInfo().getTrack() << (32 - everDaysTrack);
		return track != 0;
	}
	
	// 记录某天付费
	public void markPay(boolean isPay) {
		getPlayerMonthInfo().setPayTrack(getPlayerMonthInfo().getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某天是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int month = getMonthMinus(statDate, targetDate);
		if (month > 31) // 超过记录的范围 直接返回false
			return false;
		return (getPlayerMonthInfo().getPayTrack() >> (month) & 1) > 0;
	}

	// 计算两个日期之间的月份之差
	private static int getMonthMinus(int date1, int date2) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) date1 * 1000);
		int yearTarget = calendar.get(Calendar.YEAR);
		int monthTarget = calendar.get(Calendar.MONTH);
		calendar.setTimeInMillis((long) date2 * 1000);
		int yearFirstLogin = calendar.get(Calendar.YEAR);
		int monthFirstLogin = calendar.get(Calendar.MONTH);
		// 实际相差的月份
		int month = (yearTarget - yearFirstLogin) * 12 + monthTarget
				- monthFirstLogin;
		return Math.abs(month);
	}

	@Override
	public String toString() {
		return "UserInfoMonthRolling2 [appId=" + appId + ", appVer=" + appVer
				+ ", platform=" + platform + ", uid=" + uid + ", " 
				+ "channel="+getPlayerMonthInfo().getChannel()+ ", "
				+ "gameRegion="+getPlayerMonthInfo().getGameRegion()+ ", "
				+ "track="+getPlayerMonthInfo().getTrack()+ ", "
				+ "payTrack="+getPlayerMonthInfo().getPayTrack()+ ", "
				+"firstLoginWeekDate=" + getPlayerMonthInfo().getFirstLoginMonthDate() + ", " 
				+ "lastLoginWeekDate=" + getPlayerMonthInfo().getLastLoginMonthDate() + ", "
				+ "firstPayWeekDate=" + getPlayerMonthInfo().getFirstPayMonthDate() + ", "
				+ "lastPayWeekDate=" + getPlayerMonthInfo().getLastPayMonthDate() + ", "
				+ "]";
	}
	
	public static void main(String[] args) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(System.currentTimeMillis());
		int date1 = (int) (calendar.getTimeInMillis() / 1000);
		calendar.add(Calendar.YEAR, -1);
		calendar.add(Calendar.MONTH, 3);
		int date2 = (int) (calendar.getTimeInMillis() / 1000);
		System.out.println(date1 + "," + date2);
		System.out.println(getMonthMinus(date1, date2));
	}
}
