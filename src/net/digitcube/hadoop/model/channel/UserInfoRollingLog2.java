package net.digitcube.hadoop.model.channel;

import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerDayInfo;

public class UserInfoRollingLog2 {
	private String appId;
	private String appVer;
	private String platform;
	private String uid;
	private String infoBase64;
	private PlayerDayInfo playerDayInfo;

	private Date scheduleTime = null;

	public UserInfoRollingLog2(Date scheduleTime) {
		if (null == scheduleTime) {
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
	};

	public UserInfoRollingLog2(Date scheduleTime, String[] args) {
		if (null == scheduleTime) {
			throw new RuntimeException(
					"Schedule time is null, construct UserInfoRollingLog failed...");
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
		// this.infoBase64 = PlayInfoUtil.playDayInfo2Base64(playerDayInfo);
		this.infoBase64 = PlayInfoUtil.playDayInfo2Base64(playerDayInfo, scheduleTime);
		return new String[] { appId, appVer, platform, uid, infoBase64 };
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

	public PlayerDayInfo getPlayerDayInfo() {
		if (playerDayInfo == null) {
			// playerDayInfo = PlayInfoUtil.playDayInfoFromStr(infoBase64);
			playerDayInfo = PlayInfoUtil.playDayInfoFromStr(infoBase64, scheduleTime);

			if (playerDayInfo == null) {
				playerDayInfo = new PlayerDayInfo();
			}
		}
		return playerDayInfo;
	}

	public void setPlayerDayInfo(PlayerDayInfo playerDayInfo) {
		this.playerDayInfo = playerDayInfo;
	}


	/**
	 * @param isLogin
	 */
	public void markLogin(boolean isLogin) {
		playerDayInfo.setTrack(playerDayInfo.getTrack() << 1
				| (isLogin ? 1 : 0));
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
		return (playerDayInfo.getTrack() >> (days) & 1) > 0;
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
		int track = playerDayInfo.getTrack() << (32 - everDaysTrack);
		return track != 0;
	}

	// 记录某天付费
	public void markPay(boolean isPay) {
		playerDayInfo.setPayTrack(playerDayInfo.getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某天是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerDayInfo.getPayTrack() >> (days) & 1) > 0;
	}
}
