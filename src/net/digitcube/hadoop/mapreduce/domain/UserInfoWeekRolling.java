package net.digitcube.hadoop.mapreduce.domain;

import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerWeekInfo;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月29日 下午7:24:49 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserInfoWeekRolling implements MapReduceVO {
	private String appID;
	private String platform;
	private String accountID;
	private String infoBase64;
	private PlayerWeekInfo playerWeekInfo;
	public final static int Index_AppID = 0;
	public final static int Index_Platform = 1;
	public final static int Index_AccountID = 2;
	public final static int Index_InfoBase64 = 3;

	private Date scheduleTime = null;
	
	/**
	 * 构造函数中增加调度时间的原因见: @UserInfoRollingLog 中说明
	 * @param scheduleTime
	 */
	public UserInfoWeekRolling(Date scheduleTime) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime; 
	};

	public UserInfoWeekRolling(Date scheduleTime, String[] args) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
		
		this.appID = args[Index_AppID];
		this.platform = args[Index_Platform];
		this.accountID = args[Index_AccountID];
		this.infoBase64 = args[Index_InfoBase64];
	}

	@Override
	public String[] toStringArray() {
		//this.infoBase64 = PlayInfoUtil.playWeek2Base64(playerWeekInfo);
		this.infoBase64 = PlayInfoUtil.playWeek2Base64(playerWeekInfo, scheduleTime);
		return new String[] { appID, platform, accountID, infoBase64 };
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

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public String getInfoBase64() {
		return infoBase64;
	}

	public void setInfoBase64(String infoBase64) {
		this.infoBase64 = infoBase64;
	}

	// 记录某周登录情况
	// public void markLogin(int targetDate, boolean isLogin) {
	// int weeks = (targetDate - playerWeekInfo.getFirstLoginWeekDate())
	// / (7 * 24 * 3600);
	// weeks = weeks < 0 ? 0 : weeks;
	// if (weeks > 31) {
	// if (isLogin) {
	// playerWeekInfo.setTrack(1 << 31
	// | (playerWeekInfo.getTrack() >> 1));
	// } else {
	// playerWeekInfo.setTrack(playerWeekInfo.getTrack() >> 1);
	// }
	// } else {
	// if (isLogin) {
	// playerWeekInfo.setTrack(1 << weeks | playerWeekInfo.getTrack());
	// }
	// }
	// }

	public void markLogin(boolean isLogin) {
		playerWeekInfo.setTrack(playerWeekInfo.getTrack() << 1
				| (isLogin ? 1 : 0));
	}

	// 判断某周是否登录过
	// public boolean isLogin(int targetDate, int statDate) {
	// // 首登的日期
	// int firstLoginDate = playerWeekInfo.getFirstLoginWeekDate();
	// // 所能记录的最大时间间隔
	// int maxTimeLen = 31 * 7 * 24 * 3600;
	// int startDate = (statDate - firstLoginDate) > maxTimeLen ? (statDate -
	// maxTimeLen)
	// : firstLoginDate;
	// // 与最早记录的天数间隔
	// int weeks = (targetDate - startDate) / (7 * 24 * 3600);
	// if (weeks < 0)
	// return false;
	// return (playerWeekInfo.getTrack() >> (weeks) & 1) > 0;
	//
	// }

	public boolean isLogin(int targetDate, int statDate) {
		// 与最新结算日期的间隔
		int weeks = (statDate - targetDate) / (7 * 24 * 3600);
		if (weeks < 0 || weeks > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerWeekInfo.getTrack() >> (weeks) & 1) > 0;
	}

	public void markPay(boolean isPay) {
		playerWeekInfo.setPayTrack(playerWeekInfo.getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某周是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int days = (statDate - targetDate) / (7 * 24 * 3600);
		if (days < 0 || days > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerWeekInfo.getPayTrack() >> (days) & 1) > 0;
	}

	// 记录某周付费
	// public void markPay(int targetDate, boolean isPay) {
	// int weeks = (targetDate - playerWeekInfo.getFirstPayWeekDate())
	// / (7 * 24 * 3600);
	// weeks = weeks < 0 ? 0 : weeks;
	// if (weeks > 31) {
	// if (isPay) {
	// playerWeekInfo.setPayTrack(1 << 31
	// | (playerWeekInfo.getPayTrack() >> 1));
	// } else {
	// playerWeekInfo.setPayTrack(playerWeekInfo.getPayTrack() >> 1);
	// }
	// } else {
	// if (isPay) {
	// playerWeekInfo.setPayTrack(1 << weeks
	// | playerWeekInfo.getPayTrack());
	// }
	// }
	// }
	//
	// // 判断某周是否付费过
	// public boolean isPay(int targetDate, int statDate) {
	// // 首登的日期
	// int firstPayDate = playerWeekInfo.getFirstPayWeekDate();
	// // 所能记录的最大时间间隔
	// int maxTimeLen = 31 * 7 * 24 * 3600;
	// int startDate = (statDate - firstPayDate) > maxTimeLen ? (statDate -
	// maxTimeLen)
	// : firstPayDate;
	// // 与最早记录的天数间隔
	// int weeks = (targetDate - startDate) / (7 * 24 * 3600);
	// if (weeks < 0)
	// return false;
	// return (playerWeekInfo.getPayTrack() >> (weeks) & 1) > 0;
	// }

}
