package net.digitcube.hadoop.common;

import java.util.Arrays;

public class EnumConstants {

	// 年龄区间分布
	public static int[] AgeInterval = { 0, // 玩家未设置年龄
			15, // 1~15 岁
			20, // 16~20 岁
			25, // 21~25 岁
			30, // 26~30 岁
			35, // 31~35 岁
			40, // 36~40 岁
			45, // 41~45 岁
			50, // 46~50 岁
			55, // 51~55 岁
			60, // 56~60 岁
			Integer.MAX_VALUE // 60 岁以上
	};

	// 单次游戏时长区间分布
	public static int[] GameSingleTimeInterval = { 4, // 1~4 秒
			10, // 5~10 秒
			30, // 11~30 秒
			60, // 31~60 秒
			180, // 1~3 分
			600, // 3~10 分
			1800, // 10~30 分
			3600, // 30~60 分
			Integer.MAX_VALUE // 60 分钟以上
	};

	// 登录时间间隔区间分布
	public static int[] LoginTimeInterval = { 3600, // 0~60 分
			2 * 3600, // 1~2小时
			3 * 3600, // 2~3小时
			4 * 3600, // 3~4小时
			5 * 3600, // 4~5小时
			8 * 3600, // 5~8小时
			12 * 3600,// 8~12小时
			24 * 3600,// 12~24小时
			2 * 24 * 3600, // 1~2 天
			3 * 24 * 3600, // 2~3 天
			7 * 24 * 3600, // 3~7 天
			14 * 24 * 3600, // 7~14 天
			30 * 24 * 3600, // 14~30 天
			Integer.MAX_VALUE // 30 天以上
	};
	// 在线天数区间分布
	public static int[] OnlineDaysInterval = { 1, 3, 7, 14, 30, 90, 180, 365,
			Integer.MAX_VALUE };
	// 在线时间区间分布 秒
	public static int[] OnlineTimeInterval = { 10, 60, 180, 600, 1800, 3600,
			7200, 14400, Integer.MAX_VALUE };
	// 周游戏次数区间分布
	private static int[] WeekLoginTimesInterval = { 1, 3, 5, 10, 20, 50, 100,
			200, Integer.MAX_VALUE };
	// 周游戏时长区间分布
	private static int[] WeekOnlineTimeInterval = { 60, 180, 600, 3600, 7200,
			14400, 21600, 36000, 54000, 72000, Integer.MAX_VALUE };
	// 月在线天数区间分布
	private static int[] MonthOnlineDayInterval = { 1, 2, 3, 4, 5, 6, 7, 14,
			21, 31 };

	// 日登录次数区间分布
	private static int[] DayLoginTimesInterval = { 1, 3, 5, 10, 20, 50,
			Integer.MAX_VALUE };

	// 首付游戏天数区间分布
	private static int[] FirstPayGameDaysInterval = { 1, // 1天以内
			3, // 2~3 天
			7 * 1, // 4~7天
			7 * 2, // 2 周(8~14 天)
			7 * 3, // 3 周(15~21 天)
			7 * 4, // 4 周(22~28 天)
			7 * 5, // 5 周(29~35 天)
			7 * 6, // 6 周(36~42 天)
			7 * 7, // 7 周(43~49 天)
			7 * 8, // 8 周(50~56 天)
			7 * 12, // 9~12 周以上(57~84 天)
			Integer.MAX_VALUE // 12 周以上
	};
	// 日付费次数
	private static int[] DayPayTimes = { 1, // 1 次
			2, // 2 次
			3, // 3 次
			4, // 4 次
			5, // 5 次
			6, // 6 次
			7, // 7 次
			8, // 8 次
			9, // 9 次
			10, // 10 次
			20, // 11~20 次
			30, // 21~30 次
			40, // 31~40 次
			50, // 41~50 次
			100,// 51~100 次
			Integer.MAX_VALUE // 100+ 次
	};
	// 日付费金额 : HBase 表中范围定义
	private static int[] DayPayAmount = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15,
			20, 30, 40, 50, 60, 70, 80, 90, 100, Integer.MAX_VALUE };
	// 日付费金额 ： mysql 表中范围定义
	// 20140806 与高境约定，两个范围定义分开
	private static int[] DayPayAmount2 = { 1, // 1 元
			2, // 2 元
			3, // 3 元
			4, // 4 元
			5, // 5 元
			10, // 6 ~ 10 元
			50, // 11 ~ 50 元
			100,// 51 ~ 100 元
			500,// 101~ 500 元
			1000,// 501~ 1000 元
			2000,// 1001~2000 元
			Integer.MAX_VALUE // 2000+ 元
	};
	// 日登陆次数
	private static int[] DayLoginTimes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			Integer.MAX_VALUE };
	// 日在线时长
	private static int[] DayOnlineTime = { 10, 20, 30, 40, 50, 60, 60 * 5,
			60 * 10, 60 * 20, 60 * 30, 60 * 40, 60 * 50, 60 * 60, 60 * 60 * 2,
			60 * 60 * 3, 60 * 60 * 4, 60 * 60 * 5, 60 * 60 * 6, 60 * 60 * 7,
			60 * 60 * 8, Integer.MAX_VALUE

	};
	// 首付金额（当前默认币种是 RMB）
	private static int[] FirstPayCurInterval = { 1 * 10, // 10 元以下
			5 * 10, // 11~50 元
			1 * 100,// 51~100 元
			2 * 100,// 101~200 元
			5 * 100,// 201~500 元
			1 * 1000,// 501~1000 元
			Integer.MAX_VALUE // 1000 元以上
	};

	// 首付等级
	private static int[] FirstPayLevelInterval = { 1 * 10, // 10 级以下
			2 * 10, // 11~20 级
			3 * 10, // 21~30 级
			4 * 10, // 31~40 级
			5 * 10, // 41~50 级
			6 * 10, // 51~60 级
			Integer.MAX_VALUE // 60 级以上
	};

	// 升级时长区间
	private static int[] UpgradeTimeInterval = { 1 * 10 * 60, // 1~10分钟
			2 * 10 * 60, // 11~20 分钟
			3 * 10 * 60, // 21~30 分钟
			1 * 3600, // 31~60 分钟
			2 * 3600, // 1~2 小时
			3 * 3600, // 2~3 小时
			4 * 3600, // 3~4 小时
			5 * 3600, // 4~5 小时
			6 * 3600, // 5~6 小时
			7 * 3600, // 6~7 小时
			8 * 3600, // 7~8 小时
			Integer.MAX_VALUE // 8 小时以上
	};

	// 单设备帐号数区间
	private static int[] AccNumPerDeviceInterval = { 0, // 未注册
			1, // 1 个
			2, // 2 个
			3, // 3 个
			4, // 4 个
			5, // 5 个
			6, // 6 个
			7, // 7 个
			10, // 8~10 个
			Integer.MAX_VALUE // >10 个
	};

	// 首充、二充、三充时间间隔
	private static int[] PayTimeInterval = { 1 * 10 * 60, // 10 分钟以内
			3 * 10 * 60, // 11~30 分钟
			1 * 3600, // 31~60 分钟
			2 * 3600, // 1~2 小时
			4 * 3600, // 2~4 小时
			6 * 3600, // 4~6 小时
			10 * 3600, // 6~10 小时
			15 * 3600, // 10~15 小时
			20 * 3600, // 15~20 小时
			30 * 3600, // 20~30 小时
			40 * 3600, // 30~40 小时
			60 * 3600, // 40~60 小时
			100 * 3600,// 60~100 小时
			Integer.MAX_VALUE // 100 小时以上
	};

	// TCL appSize 大小范围
	private static int[] appSizeInterval = { 1, // 1 MB 以内
			5, // 1~5 MB
			10, // 5~10 MB
			20, // 10~20 MB
			30, // 20~30 MB
			40, // 30~40 MB
			50, // 40~50 MB
			60, // 50~60 MB
			80, // 60~80 MB
			100, // 80~100 MB
			120, // 100~120 MB
			150, // 120~150 MB
			200, // 150~200 MB
			Integer.MAX_VALUE // 200 MB以上
	};

	//下载次数
	private static int[] downloadTimes = { 
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20, Integer.MAX_VALUE
	};
	
	// 年龄区间与分布
	public static int getRangeTop4Age(int age) {
		return getRangeTop(age, AgeInterval);
	}

	// 游戏时段区间分布
	public static int getRangeTop4SingleTime(int onlineTime) {
		return getRangeTop(onlineTime, GameSingleTimeInterval);
	}

	// 两次登录时间间隔区间分布
	public static int getRangeTop4LoginTime(int timeInterval) {
		return getRangeTop(timeInterval, LoginTimeInterval);
	}

	// 获取在线天数区间
	public static int getItval4OnlineDays(int onlineDays) {
		/*
		 * int result = Arrays.binarySearch(OnlineDaysInterval, onlineDays); int
		 * index = Math.abs(result < 0 ? result + 1 : result); return
		 * OnlineDaysInterval[index];
		 */
		return getRangeTop(onlineDays, OnlineDaysInterval);
	}

	// 获取在线时间区间
	public static int getItval4OnlineTime(int onlineTime) {
		/*
		 * int result = Arrays.binarySearch(OnlineTimeInterval, onlineTime); int
		 * index = Math.abs(result < 0 ? result + 1 : result); return
		 * OnlineTimeInterval[index];
		 */
		return getRangeTop(onlineTime, OnlineTimeInterval);
	}

	// 获取周登陆次数的区间
	public static int getItval4WeekLoginTimes(int loginTimes) {
		/*
		 * int result = Arrays.binarySearch(WeekLoginTimesInterval, loginTimes);
		 * int index = Math.abs(result < 0 ? result + 1 : result); return
		 * WeekLoginTimesInterval[index];
		 */
		return getRangeTop(loginTimes, WeekLoginTimesInterval);
	}

	// 获取周在线时长的区间
	public static int getItval4WeekOnlineTime(int onlineTime) {
		/*
		 * int result = Arrays.binarySearch(WeekOnlineTimeInterval, onlineTime);
		 * int index = Math.abs(result < 0 ? result + 1 : result); return
		 * WeekOnlineTimeInterval[index];
		 */
		return getRangeTop(onlineTime, WeekOnlineTimeInterval);
	}

	// 获取月在线天数的区间
	public static int getItval4MonthOnlineDay(int onlineDay) {
		/*
		 * int result = Arrays.binarySearch(MonthOnlineDayInterval, onlineDay);
		 * int index = Math.abs(result < 0 ? result + 1 : result); return
		 * MonthOnlineDayInterval[index];
		 */
		return getRangeTop(onlineDay, MonthOnlineDayInterval);
	}

	// 获取日登录次数区间
	public static int getItval4DayLoginTimes(int loginTimes) {
		/*
		 * int result = Arrays.binarySearch(DayLoginTimesInterval, loginTimes);
		 * int index = Math.abs(result < 0 ? result + 1 : result); return
		 * DayLoginTimesInterval[index];
		 */
		return getRangeTop(loginTimes, DayLoginTimesInterval);
	}

	// 获取升级时长区间
	public static int getItval4UpgradeTime(int duration) { // duration 单位必须是秒
		return getRangeTop(duration, UpgradeTimeInterval);
	}

	// 获取单设备数区间
	public static int getAccNumPerDevRange(int accountNum) {
		return getRangeTop(accountNum, AccNumPerDeviceInterval);
	}

	// 获取首付时游戏天数期间
	public static int getFirstPayGameDaysRange(int gameDays) {
		return getRangeTop(gameDays, FirstPayGameDaysInterval);
	}

	// 获取付费时间期间
	public static int getPayTimeRange(int payTime) {
		return getRangeTop(payTime, PayTimeInterval);
	}

	// 获取首付金额期间
	public static int getFirstPayCurRange(int currency) {
		return getRangeTop(currency, FirstPayCurInterval);

	}

	// 获取游戏等级期间
	public static int getFirstPayLevelRange(int level) {
		return getRangeTop(level, FirstPayLevelInterval);
	}

	/**
	 * 获取日付费区间 DayPayAmount 区间用于 HBase 聚类分析
	 */
	public static int getDayPayAmountRange(int payAmount) {
		return getRangeTop(payAmount, DayPayAmount);
	}

	/**
	 * 获取日付费区间 DayPayAmount2 区间用于 WEB 前端展示
	 */
	public static int getDayPayAmountRange2(int payAmount) {
		return getRangeTop(payAmount, DayPayAmount2);
	}

	// 获取日付费次数区间
	public static int getDayPayTimesRange(int payAmount) {
		return getRangeTop(payAmount, DayPayTimes);
	}

	// 获取日在线时长区间
	public static int getDayOnlineTimeRange(int onlineTime) {
		return getRangeTop(onlineTime, DayOnlineTime);
	}

	// 获取日登陆次数区间
	public static int getDayLoginTimesRange(int loginTimes) {
		return getRangeTop(loginTimes, DayLoginTimes);
	}

	// 获取 APP 大小范围区间(单位是 byte)
	public static int getAppSizeRange(long appSize) {
		int size = (int) (appSize / 1024 / 1024);// 转换成 MB
		return getRangeTop(size, appSizeInterval);
	}

	// 获取玩家下载次数区间
	public static int getDownloadTimesRange(int times) {
		return getRangeTop(times, downloadTimes);
	}
	
	/**
	 * 给定一个数值和排序好的数组，返回数组中离该数值最近的比它大的值 若给定值小于给定数组中的最小值，则返回数组中的最小值
	 * 
	 * @param val
	 * @param rangeArr
	 * @return
	 */
	public static int getRangeTop(int val, int[] rangeArr) {
		int result = Arrays.binarySearch(rangeArr, val);
		int index = Math.abs(result < 0 ? result + 1 : result);
		return rangeArr[index];
	}

	public static void main(String[] args) {
		/*
		 * int result = Arrays.binarySearch(OnlineDaysInterval, 0);
		 * System.out.println(result); System.out.println();
		 */

		/*
		 * System.out.println(getItval4DayLoginTimes(-1));
		 * System.out.println(getItval4DayLoginTimes(6));
		 * System.out.println(getItval4DayLoginTimes(10));
		 * System.out.println(getItval4DayLoginTimes(29));
		 * System.out.println(getItval4DayLoginTimes(150));
		 */
		System.out.println(getRangeTop4Age(16));
	}
}
