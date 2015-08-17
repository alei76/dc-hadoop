package net.digitcube.hadoop.mapreduce.domain;

import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerMonthInfo;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月31日 下午9:04:05 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserInfoMonthRolling implements MapReduceVO {
	private String appID;
	private String platform;
	private String accountID;
	private String infoBase64;
	private PlayerMonthInfo playerMonthInfo;
	public final static int Index_AppID = 0;
	public final static int Index_Platform = 1;
	public final static int Index_AccountID = 2;
	public final static int Index_InfoBase64 = 3;

	private Date scheduleTime = null;
	
	/**
	 * 构造函数中增加调度时间的原因见: @UserInfoRollingLog 中说明
	 * @param scheduleTime
	 */
	public UserInfoMonthRolling(Date scheduleTime) {
		if(null == scheduleTime){
			throw new RuntimeException("Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
	};

	public UserInfoMonthRolling(Date scheduleTime, String[] args) {
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
		//this.infoBase64 = PlayInfoUtil.playMonth2Base64(playerMonthInfo);
		this.infoBase64 = PlayInfoUtil.playMonth2Base64(playerMonthInfo, scheduleTime);
		
		return new String[] { appID, platform, accountID, infoBase64 };
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

	// 记录某月登录情况
	// public void markLogin(int targetDate, boolean isLogin) {
	// // 当前月与首登月
	// int month = getMonthMinus(targetDate,
	// playerMonthInfo.getFirstLoginMonthDate());
	// if (month > 31) {
	// if (isLogin) {
	// playerMonthInfo.setTrack(1 << 31
	// | (playerMonthInfo.getTrack() >> 1));
	// } else {
	// playerMonthInfo.setTrack(playerMonthInfo.getTrack() >> 1);
	// }
	// } else {
	// if (isLogin) {
	// playerMonthInfo.setTrack(1 << month
	// | playerMonthInfo.getTrack());
	// }
	// }
	// }

	public void markLogin(boolean isLogin) {
		playerMonthInfo.setTrack(playerMonthInfo.getTrack() << 1
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
		return (playerMonthInfo.getTrack() >> (month) & 1) > 0;
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
		int track = playerMonthInfo.getTrack() << (32 - everDaysTrack);
		return track != 0;
	}
	
	// 判断某月是否登录过
	// public boolean isLogin(int targetDate, int statDate) {
	// int month1 = getMonthMinus(statDate,
	// playerMonthInfo.getFirstLoginMonthDate());
	// int month2 = getMonthMinus(statDate, targetDate);
	// if (month2 > 31)
	// return false;
	// int len = month1 > 31 ? 31 - month2 : month1 - month2;
	// return (playerMonthInfo.getTrack() >> (len) & 1) > 0;
	// }
	// 记录某天付费
	public void markPay(boolean isPay) {
		playerMonthInfo.setPayTrack(playerMonthInfo.getPayTrack() << 1
				| (isPay ? 1 : 0));
	}

	// 判断某天是否付费过
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int month = getMonthMinus(statDate, targetDate);
		if (month > 31) // 超过记录的范围 直接返回false
			return false;
		return (playerMonthInfo.getPayTrack() >> (month) & 1) > 0;
	}

	// 记录某周付费
	// public void markPay(int targetDate, boolean isPay) {
	// // 当前月与首登月
	// int month = getMonthMinus(targetDate,
	// playerMonthInfo.getFirstPayMonthDate());
	// month = month < 0 ? 0 : month;
	// if (month > 31) {
	// if (isPay) {
	// playerMonthInfo.setPayTrack(1 << 31
	// | (playerMonthInfo.getPayTrack() >> 1));
	// } else {
	// playerMonthInfo.setPayTrack(playerMonthInfo.getPayTrack() >> 1);
	// }
	// } else {
	// if (isPay) {
	// playerMonthInfo.setPayTrack(1 << month
	// | playerMonthInfo.getPayTrack());
	// }
	// }
	// }

	// 判断某周是否付费过
	// public boolean isPay(int targetDate, int statDate) {
	// int month1 = getMonthMinus(statDate,
	// playerMonthInfo.getFirstPayMonthDate());
	// int month2 = getMonthMinus(statDate, targetDate);
	// if (month2 > 31)
	// return false;
	// int len = month1 > 31 ? 31 - month2 : month1 - month2;
	// return (playerMonthInfo.getPayTrack() >> (len) & 1) > 0;
	// }

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
