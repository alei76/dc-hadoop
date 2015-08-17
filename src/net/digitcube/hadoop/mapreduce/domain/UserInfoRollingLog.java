package net.digitcube.hadoop.mapreduce.domain;

import java.util.Date;

import net.digitcube.hadoop.jce.PlayInfoUtil;
import net.digitcube.hadoop.jce.PlayerDayInfo;

/**
 * 用户信息每日滚存数据
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月18日 上午11:28:23 @copyrigt www.digitcube.net
 */
public class UserInfoRollingLog implements MapReduceVO {
	private String appID;
	private String platform;
	private String accountID;
	private String infoBase64;
	private PlayerDayInfo playerDayInfo;
	public final static int Index_AppID = 0;
	public final static int Index_Platform = 1;
	public final static int Index_AccountID = 2;
	public final static int Index_InfoBase64 = 3;

	private Date scheduleTime = null;

	/**
	 * Hadoop 集群中有用到第三方 jar 包 dc-protocol.jar 该 jar 包主要用 JCE 协议对日、周、月滚存的用户信息进行存取
	 * 由于历史原因，2013.11.08 之前，该 jar 包内置默认的编码是 GBK 而客户端 SDK 以及 LogServer 都已升级为
	 * UTF-8 编码，为一致性考虑 这里把 Hadoop 集群的 dc-protocol.jar 内置默认编码改为 UTF-8
	 * 
	 * JCE 编码修改后，为了对历史数据的兼容 这里需对日、周、月滚存设定一个时间点，与这些任务的调度时间比较做兼容处理： a)如果调度时间 <
	 * 设定时间点，那么 JCE 输入输出都用 GBK 编码 b)如果调度时间 = 设定时间点，那么 JCE 输入用 GBK 编码，输出用 UTF-8
	 * 编码 c)如果调度时间 > 设定时间点，那么 JCE 输入输出都用 UTF-8 编码
	 * 
	 * 所以这里构造 UserInfoRollingLog 时必须传入任务的调度时间 在未来某个时间 scheduleTime
	 * 这个参数需要去掉（某个时间是不需要再跟踪 GBK 这部分历史数据时） 因为调度时间放在 UserInfoRollingLog 这样的 VO
	 * 里不太合适
	 * 
	 * @param scheduleTime
	 */
	public UserInfoRollingLog(Date scheduleTime) {
		if (null == scheduleTime) {
			throw new RuntimeException(
					"Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;
	};

	public UserInfoRollingLog(Date scheduleTime, String[] args) {
		if (null == scheduleTime) {
			throw new RuntimeException(
					"Schedule time is null, construct UserInfoRollingLog failed...");
		}
		this.scheduleTime = scheduleTime;

		this.appID = args[Index_AppID];
		this.platform = args[Index_Platform];
		this.accountID = args[Index_AccountID];
		this.infoBase64 = args[Index_InfoBase64];
	}

	@Override
	public String[] toStringArray() {
		// this.infoBase64 = PlayInfoUtil.playDayInfo2Base64(playerDayInfo);
		this.infoBase64 = PlayInfoUtil.playDayInfo2Base64(playerDayInfo,
				scheduleTime);
		return new String[] { appID, platform, accountID, infoBase64 };
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

	public PlayerDayInfo getPlayerDayInfo() {
		if (playerDayInfo == null) {
			// playerDayInfo = PlayInfoUtil.playDayInfoFromStr(infoBase64);
			playerDayInfo = PlayInfoUtil.playDayInfoFromStr(infoBase64,
					scheduleTime);

			if (playerDayInfo == null) {
				playerDayInfo = new PlayerDayInfo();
			}
		}
		return playerDayInfo;
	}

	public void setPlayerDayInfo(PlayerDayInfo playerDayInfo) {
		this.playerDayInfo = playerDayInfo;
	}

	// 记录某天登录情况
	// public void markLogin(int targetDate, boolean isLogin) {
	// int days = (targetDate - playerDayInfo.getFirstLoginDate())
	// / (24 * 3600);
	// days = days < 0 ? 0 : days;
	// if (days > 31) {
	// if (isLogin) {
	// playerDayInfo.setTrack(1 << 31
	// | (playerDayInfo.getTrack() >> 1));
	// } else {
	// playerDayInfo.setTrack(playerDayInfo.getTrack() >> 1);
	// }
	// } else {
	// if (isLogin) {
	// playerDayInfo.setTrack(1 << days | playerDayInfo.getTrack());
	// }
	// }
	// }

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

	// 判断某天是否登录过
	// public boolean isLogin(int targetDate, int statDate) {
	// // 首登的日期
	// int firstLoginDate = playerDayInfo.getFirstLoginDate();
	// // 所能记录的最大时间间隔
	// int maxTimeLen = 31 * 24 * 3600;
	// int startDate = (statDate - firstLoginDate) > maxTimeLen ? (statDate -
	// maxTimeLen)
	// : firstLoginDate;
	// // 与最早记录的天数间隔
	// int days = (targetDate - startDate) / (24 * 3600);
	// if (days < 0)
	// return false;
	// return (playerDayInfo.getTrack() >> (days) & 1) > 0;
	// }

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

	// public void markPay(int targetDate, boolean isPay) {
	// int days = (targetDate - playerDayInfo.getFirstLoginDate())
	// / (24 * 3600);
	// days = days < 0 ? 0 : days;
	// if (days > 31) {
	// if (isPay) {
	// playerDayInfo.setPayTrack(1 << 31
	// | (playerDayInfo.getPayTrack() >> 1));
	// } else {
	// playerDayInfo.setPayTrack(playerDayInfo.getPayTrack() >> 1);
	// }
	// } else {
	// if (isPay) {
	// playerDayInfo.setPayTrack(1 << days
	// | playerDayInfo.getPayTrack());
	// }
	// }
	// }
	//
	// // 判断某天是否付费过
	// public boolean isPay(int targetDate, int statDate) {
	// // 首付日期
	// int firstPayDate = playerDayInfo.getFirstPayDate();
	// // 所能记录的最大时间间隔
	// int maxTimeLen = 31 * 24 * 3600;
	// int startDate = (statDate - firstPayDate) > maxTimeLen ? (statDate -
	// maxTimeLen)
	// : firstPayDate;
	// // 与最早记录的天数间隔
	// int days = (targetDate - startDate) / (24 * 3600);
	// if (days < 0)
	// return false;
	// return (playerDayInfo.getPayTrack() >> (days) & 1) > 0;
	// }

	//
	// // 取某天的登陆次数
	// // lastStatDate 最后结算日期，目标日期loginDate
	// public byte getLoginTimes(int lastStatDate, int loginDate) {
	// int daysFromFirstLogin = (loginDate - playerDayInfo.getFirstLoginDate())
	// / (24 * 3600);
	// int daysFromLastStat = (lastStatDate - loginDate) / (24 * 3600);
	// if (daysFromFirstLogin < 0 || daysFromLastStat < 0
	// || daysFromLastStat > playerDayInfo.getTrack().length) { //
	// 早于首登，晚于结算，超过结算长度
	// return -1;
	// }
	// byte[] track = playerDayInfo.getTrack();
	// return track[track.length - 1 - daysFromLastStat];
	// }
	// 判断某天是否登录过
	// public static boolean isLoginTest(int targetDate, int statDate,
	// int fistLogin, int track) {
	//
	// // 所能记录的最大时间间隔
	// int maxTimeLen = 31 * 24 * 3600;
	// int startDate = (statDate - fistLogin) > maxTimeLen ? (statDate -
	// maxTimeLen)
	// : fistLogin;
	// // 与最早记录的天数间隔
	// int days = (targetDate - startDate) / (24 * 3600);
	//
	// return (track >> (days) & 1) > 0;
	// }
	//
	// // 0-39天
	// public final static void main(String[] args) {
	// int i = 10;
	// int statDate = 1375372800;
	// PlayerDayInfo playerDayInfo = PlayInfoUtil
	// .playDayInfoFromStr("H4sIAAAAAAAAAGMKfKfUIAQilAJ_mjQYBf5qY3BgCmBJZN1RwNTAMpF1x0JGrQ0sB5gusDxk3fGRn1HrgwDLJ0GQ2k9CIMUfhYFCIiwfRFm_iXEmZyTm5aXmeKZ8E-dKT8xNDU4tKkst-iDB_EGSGQAsTLPLagAAAA..");
	//
	// while ((--i) > 0) {
	// int targetDate = statDate - i * 24 * 3600;
	// boolean isLogin = isLoginTest(targetDate, statDate,
	// playerDayInfo.getFirstLoginDate(), playerDayInfo.getTrack());
	// System.out.println(i + ":" + targetDate + ":" + isLogin);
	//
	// }
	// }
	public static String parseInt2Binary(int intVal) {
		String s = "";
		for (int i = 31; i >= 0; i--) {
			s += intVal >> i & 1;
		}
		return s;
	}

	public final static void main(String[] args) {
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(
				new Date());
		userInfoRollingLog
				.setInfoBase64("H4sIAAAAAAAAAGMKyY2xEgIRSiE5-xqMQIQDYwB7orpaAWMD-0R1tQVsGxgPMF5gf6iu9oGf7YMA4ydBkKJPQiDygzDbBxHGP6LfxJi8I7-Js8c7-vjEuwd_kGD8IMn4UUrp_k9ZBkYuJpBSQXU1BXYDNgdG7k9yIblvKr_Jqxi4GDm6uZkZ61oYmhvqmji7Gus6Ojk56VoYu7lZWloYuppYOn9TYHuyY-3T2Xu_KXI-3bn_yY45z-c0_lH6o_xNhdHgmypnZkBGfl6qiY7hNzVWcz1DPaNv6uxmJgZalmYG3zR4IFqfT9n6ZP_CD5pMH7TYPmmH5L6VAwCy9iLa9wAAAA..");

		System.out.println(userInfoRollingLog.getPlayerDayInfo());
	}
}
