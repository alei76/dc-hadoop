package net.digitcube.hadoop.common;

public interface Constants {

	// -----------字段的 index ----------
	// 公共头部
	public static final int INDEX_TIMESTAMP = 0;
	public static final int INDEX_APPID = 1;
	public static final int INDEX_UID = 2;
	public static final int INDEX_ACCOUNTID = 3;
	public static final int INDEX_PLATFORM = 4;
	public static final int INDEX_CHANNEL = 5;
	public static final int INDEX_ACCOUNTTYPE = 6;
	public static final int INDEX_GENDER = 7;
	public static final int INDEX_AGE = 8;
	public static final int INDEX_GAMESERVER = 9;

	public static final int INDEX_RESOLUTION = 10;
	public static final int INDEX_OPERSYSTEM = 11;
	public static final int INDEX_BRAND = 12;
	public static final int INDEX_NETTYPE = 13;
	public static final int INDEX_COUNTRY = 14;
	public static final int INDEX_PROVINCE = 15;
	public static final int INDEX_OPERATORS = 16;

	// 注册激活字段
	public static final int INDEX_ACTTIME = 17;
	public static final int INDEX_REGTIME = 18;

	// 在线字段
	public static final int INDEX_LOGIN_TIME = 17;
	public static final int INDEX_ONLINE_TIME = 18;
	public static final int INDEX_ONLINE_LEVEL = 19;

	// 广告效果追踪的特殊渠道ID
	// 20141202 DataEye_IOS_Channel --> DataEye_Channel_For_Track
	// public static final String Channle_For_Track = "DataEye_IOS_Channel";
	public static final String Channle_For_Track = "DataEye_Channel_For_Track";

	// -----------输出后缀 ----------
	// 注册激活
	public static final String SUFFIX_ACT = "ACT";
	public static final String SUFFIX_REG = "REG";
	public static final String SUFFIX_USERINFO_DAY = "USERINFO_DAY";

	// h5注册激活
	public static final String SUFFIX_H5_USERINFO_DAY = "UserInfo";

	// 小时注册激活日志
	public static final String SUFFIX_USERINFO_HOUR = "UserInfo";
	// 小时点击日志
	public static final String SUFFIX_CLICK_HOUR = "ClickLog";

	// online
	public static final String SUFFIX_ONLINE_HOUR = "ONLINE_HOUR";
	public static final String SUFFIX_ONLINE_DAY = "ONLINE_DAY";
	public static final String SUFFIX_ONLINE_FIRST_DAY = "ONLINE_FIRST_DAY";

	@Deprecated
	public static final String SUFFIX_ONLINE_NATURE_WEEK = "ONLINE_NATURE_WEEK";
	@Deprecated
	public static final String SUFFIX_ONLINE_NATURE_MONTH = "ONLINE_NATURE_MONTH";

	@Deprecated
	public static final String SUFFIX_ACU_HOUR = "ACU_HOUR";
	@Deprecated
	public static final String SUFFIX_ACU_HOUR_APP = "ACU_HOUR_APP";
	@Deprecated
	public static final String SUFFIX_PCU_HOUR = "PCU_HOUR";
	@Deprecated
	public static final String SUFFIX_PCU_HOUR_APP = "PCU_HOUR_APP";
	@Deprecated
	public static final String SUFFIX_PLAYER_COUNT_HOUR_APP = "PLAYER_COUNT_HOUR_APP";

	// ERROR log
	public static final String SUFFIX_ERROR_LOG = "ERROR_LOG";

	// ACU/PCU/ONLINE COUNT 统计中间结果
	public static final String SUFFIX_ACU_PCU_CNT = "ACU_PCU_CNT";
	public static final String SUFFIX_ACU_PCU_CNT_SUM = "ACU_PCU_CNT_SUM";
	// payment
	public static final String SUFFIX_PAYMENT_DAY = "PAYMENT_DAY";
	public static final String SUFFIX_PAYMENT_DAY_APP = "PAYMENT_DAY_APP";
	public static final String SUFFIX_NEW_PLAYER_PAY_APP = "NEW_PLAYER_PAY_APP";
	@Deprecated
	public static final String SUFFIX_PAYMENT_NATURE_WEEK = "PAYMENT_NATURE_WEEK";
	@Deprecated
	public static final String SUFFIX_PAYMENT_NATURE_MONTH = "PAYMENT_NATURE_MONTH";
	// 收入在玩家级别上的分布（某一级别的收入总金额和付费人次）
	public static final String SUFFIX_PAYMENT_LAYOUT_ON_LEVEL = "PAYMENT_LAYOUT_ON_LEVEL";

	// userroll
	public static final String SUFFIX_USERROLLING = "USERROLLING_DAY";
	// Add at 20140826 道具、关卡、任务扩展滚存
	// public static final String SUFFIX_EXT_INFO_ROLL_DAY =
	// "EXT_INFO_ROLL_DAY";
	public static final String SUFFIX_EXT_INFO_ROLL_DAY = "EXT_INFO_ROLL_DAY_NEW";
	public static final String SUFFIX_EXT_INFO_ROLLING = "EXT_INFO_ROLLING";

	// 用户流动 流失/回流/留存
	public static final String SUFFIX_USERFLOW = "USER_FLOW_DAY";
	// 用户流动 流失/回流/留存
	public static final String SUFFIX_USERFLOW_APP = "USER_FLOW_DAY_APP";
	// 流失漏斗
	public static final String SUFFIX_USER_LOST_FUNNEL = "USER_LOST_FUNNEL_DAY";
	// 流失漏斗 for app
	public static final String SUFFIX_USER_LOST_FUNNEL_APP = "USER_LOST_FUNNEL_DAY_APP";
	// 周流失漏斗
	public static final String SUFFIX_USER_LOST_FUNNEL_WEEK = "USER_LOST_FUNNEL_WEEK";
	// 周流失漏斗 for app
	public static final String SUFFIX_USER_LOST_FUNNEL_WEEK_APP = "USER_LOST_FUNNEL_WEEK_APP";
	// 1 周流失
	public static final String SUFFIX_USER_LOST_WEEK_1 = "USER_LOST_WEEK_1";
	// 1 周回流
	public static final String SUFFIX_USER_BACK_WEEK_1 = "USER_BACK_WEEK_1";
	// 月流失漏斗
	public static final String SUFFIX_USER_LOST_FUNNEL_MONTH = "USER_LOST_FUNNEL_MONTH";
	// 月流失漏斗 for app
	public static final String SUFFIX_USER_LOST_FUNNEL_MONTH_APP = "USER_LOST_FUNNEL_MONTH_APP";
	// 1 月流失
	public static final String SUFFIX_USER_LOST_MONTH_1 = "USER_LOST_MONTH_1";
	// 1 月回流
	public static final String SUFFIX_USER_BACK_MONTH_1 = "USER_BACK_MONTH_1";

	// 玩家在线信息（包括新增、活跃、付费，从滚存中输出）
	public static final String SUFFIX_PLAYER_ONLINE_INFO = "PLAYER_ONLINE_INFO";
	// 玩家付费信息（包括付费金额、次数等）
	public static final String SUFFIX_PLAYER_PAY_INFO = "PLAYER_PAY_INFO";

	// 从滚存中输出的用于新增玩家日期修正
	public static final String SUFFIX_NEW_PLAYER_REVISE = "NEW_PLAYER_REVISE";

	// 设备 - 新增用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_DEVICE_NEWADD = "LAYOUT_DEVICE_NEWADD";
	// 设备 - 付费用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_DEVICE_PAYMENT = "LAYOUT_DEVICE_PAYMENT";
	// 设备 - 活跃用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_DEVICE_ONLINE = "LAYOUT_DEVICE_ONLINE";

	// 玩家习惯
	public static final String SUFFIX_USER_HABITS = "USER_HABITS_DAY";
	// 玩家习惯

	public static final String SUFFIX_USER_HABITS_APP = "USER_HABITS_DAY_APP";
	public static final String SUFFIX_USER_HABITS_WEEK_APP = "USER_HABITS_WEEK_APP";
	public static final String SUFFIX_USER_HABITS_MONTH_APP = "USER_HABITS_MONTH_APP";
	// 玩家分布-在线天数
	public static final String SUFFIX_USER_LAYOUT_DAYS = "USER_LAYOUT_DAY_DAYS";
	public static final String SUFFIX_USER_LAYOUT_WEEK = "USER_LAYOUT_DAY_WEEK";
	public static final String SUFFIX_USER_LAYOUT_MONTH = "USER_LAYOUT_DAY_MONTH";
	// 玩家分布-在线时长
	public static final String SUFFIX_USER_LAYOUT_TIME = "USER_LAYOUT_DAY_TIME";
	// 玩家分布-等级
	public static final String SUFFIX_USER_LAYOUT_LEVEL = "USER_LAYOUT_DAY_LEVEL";

	// 玩家习惯
	public static final String SUFFIX_OSS = "OSS";

	public static final String SUFFIX_DISTRIBUE_STATISTICS = "DIST_STATISTICS";

	// 周滚存数据
	public static final String SUFFIX_USERROLLING_EVERY_WEEK = "USERROLLING_EVERY_WEEK";
	public static final String SUFFIX_USERROLLING_WEEK = "USERROLLING_WEEK";
	// 月滚存数据
	public static final String SUFFIX_USERROLLING_EVERY_MONTH = "USERROLLING_EVERY_MONTH";
	public static final String SUFFIX_USERROLLING_MONTH = "USERROLLING_MONTH";

	// 激活设备中的新增玩家
	public static final String SUFFIX_ACT_DEV_FIR_ONLINE = "ACT_DEV_FIR_ONLINE";
	public static final String SUFFIX_NEW_ACTDEVICE_PLAYER = "NEW_ACTDEVICE_PLAYER";
	// 新增玩家新增付费玩家
	public static final String SUFFIX_NEWADD_NEWPAY_PLAYER = "NEWADD_NEWPAY_PLAYER";

	// -1/7/30日活跃 新增首日，首周，首月付费数
	public static final String SUFFIX_1730_ACT_PAY_ROLLING = "1730_ACT_PAY";
	public static final String SUFFIX_1730_ACT_PAY = "1730_ACT_PAY_SUM";

	// 玩家分布统计
	// 新增用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_PLAYER_NEWADD = "LAYOUT_PLAYER_NEWADD";
	// 活跃用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_PLAYER_ONLINE = "LAYOUT_PLAYER_ONLINE";
	// 付费用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_PLAYER_PAYMENT = "LAYOUT_PLAYER_PAYMENT";

	// 玩家游戏时段和单次游戏时长统计
	// 新增用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_TIME_NEWADD = "LAYOUT_TIME_NEWADD";
	// 活跃用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_TIME_ONLINE = "LAYOUT_TIME_ONLINE";
	// 付费用户
	@Deprecated
	public static final String SUFFIX_LAYOUT_TIME_PAYMENT = "LAYOUT_TIME_PAYMENT";

	// ----------- 分布情况统计后缀 ----------
	// 设备分布情况统计
	public static final String SUFFIX_LAYOUT_ON_DEVICE = "LAYOUT_ON_DEVICE";
	// 收入分布情况统计
	public static final String SUFFIX_LAYOUT_ON_INCOME = "LAYOUT_ON_INCOME";
	// 玩家属性分布情况统计
	public static final String SUFFIX_LAYOUT_ON_PLAYER = "LAYOUT_ON_PLAYER";

	// 游戏时段、单次游戏时长分布统计
	public static final String SUFFIX_LAYOUT_ON_TIME = "LAYOUT_ON_TIME";

	// 首付信息：包含首付游戏天数、游戏时长、首付金额、首付等级、首付充值包
	// UserRollingDay 中输出
	public static final String SUFFIX_FIRST_PAY_DAY = "FIRST_PAY_DAY";
	// 首付游戏天数、游戏时长、首付金额、首付等级、首付充值包分布统计
	public static final String SUFFIX_LAYOUT_ON_FIRST_PAY = "LAYOUT_ON_FIRST_PAY";
	// 玩家首付金额
	public static final String SUFFIX_FIRST_PAY_CURRENCY = "FIRST_PAY_CURRENCY";

	// 统计新增玩家、付费玩家当天游戏时段寒夜单次游戏时长时
	// 需先使他们和活跃玩家进行关联，得到游戏在线信息（因为这些信息都存在活跃玩家中）
	//
	// 新增玩家、付费玩家和活跃玩家关联结果后缀
	public static final String SUFFIX_PAY_NEW_UNION_ACT = "PAY_NEW_UNION_ACT";

	// 当天激活设备、新增玩家、激活设备中新增玩家、新增付费玩家统计后缀
	// public static final String SUFFIX_DEVICE_NEW_PAY_COUNT =
	// "DEVICE_NEW_PAY_COUNT";
	public static final String SUFFIX_NEW_ADD_STATISTICS = "NEW_ADD_STATISTICS";
	// 当天激活设备及激活设备中的新增玩家
	public static final String SUFFIX_NEW_ADD_DEV_PLAYER = "NEW_ADD_DEV_PLAYER";

	// 事件统计
	public static final String SUFFIX_EVENT_STAT = "EVENT_STAT";
	// 事件统计
	public static final String SUFFIX_EVENT_ATTR_STAT = "EVENT_ATTR_STAT";
	// 升级时长
	public static final String SUFFIX_EVENT_LEVEL_UP = "EVENT_LEVEL_UP";

	// Added at 20140811: 登录前上报事件特定 ID
	public static final String DESelf_BeforeLogin_Event = "_DESelf_BeforeLogin_Event";

	// 体验数据上报事件ID
	public static final String DESelf_GE_SceneInfo_Event = "_DESelf_GE_SceneInfo";
	// 场景总时长 、上行、下行
	public static final String SUFFIX_SCENEINFO_DURATIONRXRT = "SCENEINFO_DURATIONRXRT";
	// 场景手势
	public static final String SUFFIX_SCENEINFO_GESTURE = "SCENEINFO_GESTURE";
	// 热力图
	public static final String SUFFIX_SCENEINFO_TOUCH = "SCENEINFO_TOUCH";
	// 性能分析
	public static final String SUFFIX_SCENEINFO_PA_FPS = "SCENEINFO_PA_FPS";
	public static final String SUFFIX_SCENEINFO_PA_CPU = "SCENEINFO_PA_CPU";
	public static final String SUFFIX_SCENEINFO_PA_RAM = "SCENEINFO_PA_RAM";
	public static final String SUFFIX_APP_SCENETREE_4_LOGINTIME = "APP_SCENETREE_4_LOGINTIME";
	public static final String SUFFIX_APP_SCENETREE_SUM = "APP_SCENETREE_SUM";

	// 场景手势统计type
	public static final String SCENEINFO_GESTURE = "sg";
	// 场景热力图type
	public static final String SCENEINFO_TOUCH = "st";
	// 场景性能分析 fps
	public static final String SCENEINFO_PA_FPS = "fps";
	// 场景性能分析 cpu
	public static final String SCENEINFO_PA_CPU = "cpu";
	// 场景性能分析 ram
	public static final String SCENEINFO_PA_RAM = "ram";
	// 场景 竖屏
	public static final String SCENEINFO_ORIENTATION_PORTRAIT = "PORTRAIT";
	// 场景 横屏
	public static final String SCENEINFO_ORIENTATION_LANDSCAPE = "LANDSCAPE";
	// 场景 分辨率 x
	public static final int SCENEINFO_RESOLUTION_X = 320;
	// 场景 分辨率 y
	public static final int SCENEINFO_RESOLUTION_Y = 480;
	// ----------- 玩家等级分布/升级时长/区间分布 ----------
	// 等级人数统计
	public static final String SUFFIX_LEVEL_STATISTICS = "LEVEL_STATISTICS";
	// 等级滞停人数统计
	public static final String SUFFIX_LEVEL_STOP_STAT = "LEVEL_STOP_STAT";
	// 升级时长
	public static final String SUFFIX_LEVEL_UP_TIME_STAT = "LEVEL_UP_TIME_STAT";
	// 升级时长区间分布
	public static final String SUFFIX_LEVEL_UP_TIMERANGE = "LEVEL_UP_TIMERANGE";
	// 单设备的帐号数
	public static final String SUFFIX_ACCOUNT_NUM_PER_DEV = "ACCOUNT_NUM_PER_DEV";

	// ------added by ivan --start
	// 用户类型的后缀
	public static final String SUFFIX_USER_TYPE = "USER_TYPE";
	// 房间数据
	public static final String SUFFIX_ROOM_DATA = "ROOM_DATA";
	// 房间局数统计
	public static final String SUFFIX_ROOM_PLAYTIMES = "ROOM_PLAYTIMES";

	public static final String SUFFIX_ROOM_COIN_GAIN_LOST = "ROOM_COIN_GAIN_LOST";
	public static final String SUFFIX_COIN_GAIN_LOST = "COIN_GAIN_LOST";
	public static final String FLAG_GAIN = "G";
	public static final String FLAG_LOST = "L";

	public static final String SUFFIX_TOTAL_COIN = "TOTAL_COIN";
	// 虚拟币消耗人数分布
	public static final String SUFFIX_COIN_LOST_PLAYER_NUM = "COIN_LOST_PLAYER_NUM";

	@Deprecated
	public static final String SUFFIX_TOTAL_COIN_EACH_ACCOUNT = "TOTAL_COIN_EACH_ACCOUNT";
	public static final String SUFFIX_COIN_GAIN_LOST_DIS = "COIN_GAIN_LOST_DIS";
	// -------added by ivan --end

	// -------错误日志详情/错误分布
	// 错误日志详情
	public static final String SUFFIX_ERROR_REPORT_DETAIL = "ERROR_REPORT_DETAIL";
	// 错误日志分布
	public static final String SUFFIX_ERROR_REPORT_DIST = "ERROR_REPORT_DIST";

	// 1/7/30 有效玩家
	public static final String SUFFIX_VALID_PLAYER = "VALID_PLAYER";
	public static final String SUFFIX_VALID_PLAYER_SUM = "VALID_PLAYER_SUM";
	public static final String SUFFIX_VALID_P_LAYOUT_DAY = "VALID_P_LAYOUT_DAY";
	public static final String SUFFIX_VALID_P_LAYOUT_WEEK = "VALID_P_LAYOUT_WEEK";
	public static final String SUFFIX_VALID_P_LAYOUT_MONTH = "VALID_P_LAYOUT_MONTH";

	// 首充、二充、三充时间间隔
	public static final String SUFFIX_PAY_TIME_INTERVAL = "PAY_TIME_INTERVAL";
	public static final String SUFFIX_PAY_TIME_INTERVAL_SUM = "PAY_TIME_INTERVAL_SUM";

	// 虚拟币
	public static final String SUFFIX_COIN_ROLLING = "COIN_ROLLING_DAY";

	// 任务与道具
	// 每个玩家道具购买汇总
	// public static final String SUFFIX_ITEM_BUY_PLAYER = "ITEM_BUY_PLAYER";
	// // 每个道具的购买情况统计
	// public static final String SUFFIX_ITEM_BUY_SUM = "ITEM_BUY_SUM";
	// // 每个玩家道具获得与消耗汇总
	// public static final String SUFFIX_ITEM_GAINLOST_PLAYER =
	// "ITEM_GAINLOST_PLAYER";
	// // 每个道具的获得及消耗情况统计
	// public static final String SUFFIX_ITEM_GAINLOST_SUM =
	// "ITEM_GAINLOST_SUM";

	public static final String SUFFIX_ITEM_BUY_PLAYER = "ITEM_BUY_PLAYER_NEW";
	// 每个道具的购买情况统计
	public static final String SUFFIX_ITEM_BUY_SUM = "ITEM_BUY_SUM_NEW";
	// 每个玩家道具获得与消耗汇总
	public static final String SUFFIX_ITEM_GAINLOST_PLAYER = "ITEM_GAINLOST_PLAYER_NEW";
	// 每个道具的获得及消耗情况统计
	public static final String SUFFIX_ITEM_GAINLOST_SUM = "ITEM_GAINLOST_SUM_NEW";

	// 道具购买
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER = "ITEM_BUY_FOR_PLAYER";
	// 道具获得
	public static final String SUFFIX_ITEM_GET_FOR_PLAYER = "ITEM_GET_FOR_PLAYER";
	// 道具消耗
	public static final String SUFFIX_ITEM_USE_FOR_PLAYER = "ITEM_USE_FOR_PLAYER";
	// 玩家道具购买、获得、消耗情况
	public static final String SUFFIX_ITEM_FOR_PLAYER = "ITEM_FOR_PLAYER";
	// 道具购买获得消耗
	public static final String SUFFIX_ITEM_SUM = "ITEM_SUM";

	// 每个任务的完成情况统计
	public static final String SUFFIX_TASK_STAT = "TASK_STAT";
	// 每个任务的完成情况在等级和失败原因上的分布
	public static final String SUFFIX_TASK_STAT_LAYOUT = "TASK_STAT_LAYOUT";
	// 统计每玩家每任务的开始成功失败次数
	public static final String SUFFIX_TASK_FOR_PLAYER = "TASK_FOR_PLAYER";

	// 每个关卡对每个玩家的汇总
	public static final String SUFFIX_GUANKA_FOR_PLAYER = "GUANKA_FOR_PLAYER";
	// 每个关卡对每个玩家的失败原因分布
	public static final String SUFFIX_GUANKA_FAIL_REASON_DIST = "GUANKA_FAIL_REASON_DIST";
	// 关卡统计汇总
	public static final String SUFFIX_GUANKA_STAT = "GUANKA_STAT";
	public static final String SUFFIX_GUANKA_PAY = "GUANKA_PAY";
	public static final String SUFFIX_GUANKA_ITEM = "GUANKA_ITEM";

	// 关卡任务数据标识
	public static final String DATA_FLAG_GUANKA_BEGIN = "B";
	public static final String DATA_FLAG_GUANKA_END = "E";
	public static final String DATA_FLAG_GUANKA_BEGIN_TIMES = "BT";
	public static final String DATA_FLAG_GUANKA_SUCCESS_TIMES = "ST";
	public static final String DATA_FLAG_GUANKA_FAILED_TIMES = "FT";
	public static final String DATA_FLAG_GUANKA_FAILED_EXIT_TIMES = "FET";
	public static final String DATA_FLAG_GUANKA_DURATION = "DU";

	// 新增玩家 30 日 贡献值和平均 ARPU
	public static final String SUFFIX_PLAYER_30DAYVALUE = "PLAYER_30DAYVALUE";
	public static final String SUFFIX_PLAYER_30DAYVALUE_SUM = "PLAYER_30DAYVALUE_SUM";
	public static final String SUFFIX_PLAYER_30DAY_ARPU = "PLAYER_30DAY_ARPU";
	public static final String SUFFIX_PLAYER_30DAY_ARPU_SUM = "PLAYER_30DAY_ARPU_SUM";

	// 鲸鱼玩家
	public static final String SUFFIX_WHALE_PLAYER = "WHALE_PLAYER";
	public static final String SUFFIX_WHALE_PLAYER_SUM = "WHALE_PLAYER_SUM";

	// 滚存入Hbase
	public static final String SUFFIX_ROLLING_IN_HBASE = "ROLLING_IN_HBASE";

	// 玩家基本信息入 HBase，包括：新增日期、收入日期、当天在线天数...
	public static final String SUFFIX_BASIC_INFO_4_HBASE = "BASIC_INFO_4_HBASE";

	// 设备留存后缀
	public static final String SUFFIX_UID_ROLLING_DAY = "UID_ROLLING_DAY";
	public static final String SUFFIX_UID_NEWADD_DAY = "UID_NEWADD_DAY";
	public static final String SUFFIX_UID_NEWADD_DAY_SUM = "UID_NEWADD_DAY_SUM";
	public static final String SUFFIX_UID_30DAY_RETAIN = "UID_30DAY_RETAIN";
	public static final String SUFFIX_UID_30DAY_RETAIN_SUM = "UID_30DAY_RETAIN_SUM";

	// -----------TCL 相关任务后缀----------
	public static final String SUFFIX_APP_PAGETREE_4_LOGINTIME = "APP_PAGETREE_4_LOGINTIME";
	public static final String SUFFIX_APP_PAGETREE_SUM = "APP_PAGETREE_SUM";
	public static final String SUFFIX_APP_PAGE_MODULE_ROLL = "APP_PAGE_MODULE_ROLL";
	public static final String SUFFIX_APP_PAGE_MODULE_4_LOSTUSER = "APP_PAGE_MODULE_4_LOSTUSER";
	public static final String SUFFIX_APP_LOSTUSER_LAST_USE_PG_MD = "APP_LOSTUSER_LAST_USE_PG_MD";
	public static final String SUFFIX_APP_PAGE_MODULE_4_LOSTUSER_SUM = "APP_PAGE_MODULE_4_LOSTUSER_SUM";
	public static final String SUFFIX_APP_PAGE_MODULE_TIMES_DUR = "APP_PAGE_MODULE_TIMES_DUR";
	public static final String SUFFIX_APP_KEYWORD_SEARCH_STAT = "APP_KEYWORD_SEARCH_STAT";
	public static final String SUFFIX_APP_VIEW_AND_DOWNLOAD = "APP_VIEW_AND_DOWNLOAD";
	// -----------TCL 统计维度分布----------
	public static final String DIMENSION_APP_PAGE = "page";
	public static final String DIMENSION_APP_FUNCTION = "fun";
	public static final String DIMENSION_APP_COUNT = "cnt";
	public static final String DIMENSION_APP_DURATION = "dur";
	public static final String DIMENSION_APP_UID_COUNT = "uidCnt";// 页面访问的设备数
	public static final String DIMENSION_APP_VIEW = "AppView";
	public static final String DIMENSION_APP_DOWNLOAD = "AppDownload";

	// -----------其它标志类 ----------
	// 分布统计类型
	// public static final String DIST_TYPE_NEW = "1";
	// public static final String DIST_TYPE_ACT = "2";

	// 数据标志：标志这是激活设备、新增玩家或首付玩家等等
	public static final String DATA_FLAG_DEVICE_ACT = "ACT_DEV";
	public static final String DATA_FLAG_FIRST_ONLINE = "FIR_ONL";
	public static final String DATA_FLAG_FIRST_PAY = "FIR_PAY";
	public static final String DATA_FLAG_AD_FONLINE = "AD_FO";
	// 新增激活设备
	public static final String DATA_FLAG_NEWACT_DEVICE = "NAD";
	// 激活设备中新增玩家
	public static final String DATA_FLAG_ACTDEV_PLAYER = "ADP";
	// 新增玩家
	public static final String DATA_FLAG_NEWADD_PLAYER = "NAP";
	// 新增付费玩家
	public static final String DATA_FLAG_NEWPAY_PLAYER = "NPP";

	// 是否激活设备中的新增玩家
	public static final String DATA_FLAG_YES = "Y";
	public static final String DATA_FLAG_NO = "N";
	// SDK 中 true 和 false 的结果标识
	public static final String DATA_FLAG_EVENT_TRUE = "1";
	public static final String DATA_FLAG_EVENT_FALSE = "0";

	// 新增玩家
	public static final String DATA_FLAG_PLAYER_NEW = "N";
	// Online 活跃玩家
	public static final String DATA_FLAG_PLAYER_ONLINE = "O";
	// New And Online 既是新增玩家也是活跃玩家
	public static final String DATA_FLAG_PLAYER_NEW_ONLINE = "NO";
	// Online And Pay Today 活跃玩家并且今天有付费
	public static final String DATA_FLAG_PLAYER_ONLINE_PAY = "OP";
	// New And Online And Pay Today 集新增、活跃、付费于一声
	public static final String DATA_FLAG_PLAYER_NEW_ONLINE_PAY = "NOP";

	// ACU/PCU/ONLINE_COUNT
	public static final String DATA_FLAG_ACU = "ACU";
	public static final String DATA_FLAG_PCU = "PCU";
	public static final String DATA_FLAG_CNT = "CNT";

	// 玩家类型标识
	public static final String PLAYER_TYPE_NEWADD = "1"; // 玩家类型标识：新增
	public static final String PLAYER_TYPE_ONLINE = "2"; // 玩家类型标识：活跃
	public static final String PLAYER_TYPE_PAYMENT = "3"; // 玩家类型标识：付费
	// Added at 20140904
	// 曾经付过费的玩家
	// 流失玩家中增加曾经付过费的玩家流失统计
	// 而 PLAYER_TYPE_PAYMENT 有些地方指当日付费，有些地方指曾经付费
	// 所以这里补充Y一个玩家类型 PLAYER_TYPE_EVER_PAY 标识曾经付费的玩家
	public static final String PLAYER_TYPE_EVER_PAY = "4"; // 曾经付过费的玩家
	// Added at 20141106
	// 在注册登录之前，玩家的操作会以默认的 accountId 上报数据
	// 一般只需统计自定义事件中的日志即可：EVENT_BEFORE_LOGIN
	public static final String NO_LOGIN_ACCOUNTID = "_DESelf_DEFAULT_ACCOUNTID";

	// 前N日活跃标识
	public static final String PLAYER_Active_1Day = "a1";
	public static final String PLAYER_Active_7Day = "a7";
	public static final String PLAYER_Active_30Day = "a30";
	// 前N日首付标识
	public static final String PLAYER_Pay_1Day = "p1";
	public static final String PLAYER_Pay_7Day = "p7";
	public static final String PLAYER_Pay_30Day = "p30";

	// 1/7/30 有效玩家: valid player
	public static final String PLAYER_VALID = "vp"; // 用于数据库维度类型标识
	public static final String PLAYER_VALID_1 = "v1";
	public static final String PLAYER_VALID_7 = "v7";
	public static final String PLAYER_VALID_30 = "v30";

	/*
	 * 设备分布统计和玩家属性统计时用于区分 仅仅用于标识，如需入库，reduce 输出时得设回原值 PLAYER_TYPE_NEWADD2 --> PLAYER_TYPE_NEWADD PLAYER_TYPE_ONLINE2 -->
	 * PLAYER_TYPE_ONLINE PLAYER_TYPE_PAYMENT2 --> PLAYER_TYPE_PAYMENT
	 */
	/*
	 * public static final String PLAYER_TYPE_NEWADD_2 = "11"; // 玩家类型标识：新增 public static final String
	 * PLAYER_TYPE_ONLINE_2 = "22"; // 玩家类型标识：活跃 public static final String PLAYER_TYPE_PAYMENT_2 = "33"; // 玩家类型标识：付费
	 */
	/*
	 * 日志文件名标志，如： 文件名中包含 Payment 表示这是付费玩家日志 文件名中包含 Payment_First 表示这是首次付费玩家日志 文件名中包含 Online_First 表示这是首次登录玩家日志
	 */
	public static final String LOG_FLAG_PAYMENT = "Payment";
	public static final String LOG_FLAG_PAYMENT_FIRST = "Payment_First";
	public static final String LOG_FLAG_ONLINE_FIRST = "Online_First";
	public static final String LOG_FLAG_ONLINE = "Online";
	public static final String LOG_FLAG_USERINFO = "UserInfo";

	// payment week or month
	public static final String KEY_PAYMENT_NATURE_WEEK = "key.payment.nature.week";
	public static final String KEY_PAYMENT_NATURE_MONTH = "key.payment.nature.month";
	public static final String KEY_ONLINE_NATURE_WEEK = "key.online.nature.week";
	public static final String KEY_ONLINE_NATURE_MONTH = "key.online.nature.month";

	// ----------- 设备分布统计维度标识 -----------
	// 机型
	public static final String DIMENSION_DEVICE_BRAND = "bd";
	// 分辨率
	public static final String DIMENSION_DEVICE_RESOL = "rl";
	// 运营商
	public static final String DIMENSION_DEVICE_OPERATOR = "op";
	// SIM 卡运营商
	public static final String DIMENSION_SIM_CARD_OPERATOR = "mop";
	// 操作系统
	public static final String DIMENSION_DEVICE_OS = "os";
	// 联网方式
	public static final String DIMENSION_DEVICE_NETTYPE = "nt";

	// 单设备帐号数(account num per device)
	public static final String DIMENSION_DEVICE_ACC_NUM_PER_DEV = "anpd";

	// ----------- 用户属性分布统计维度标识 -----------
	// 渠道
	public static final String DIMENSION_PLAYER_CHANNEL = "ch";
	// 地区(省份)
	public static final String DIMENSION_PLAYER_AREA = "ar";
	// 地区(国家)
	public static final String DIMENSION_PLAYER_COUNTRY = "cntry";
	// 性别
	public static final String DIMENSION_PLAYER_GENDER = "gd";
	// 帐号类型
	public static final String DIMENSION_PLAYER_ACCOUNTTYPE = "at";
	// 年龄
	public static final String DIMENSION_PLAYER_AGE = "ag";
	// 单次游戏时长
	public static final String DIMENSION_PLAYER_SINGLETIME = "st";
	// 单次游戏时长人数分布
	public static final String DIMENSION_PLAYER_SINGLETIME_PlayerNum = "stpNum";
	// 游戏时段
	public static final String DIMENSION_PLAYER_TIMEPOINT = "tp";
	// 首次游戏时长
	public static final String DIMENSION_FIRST_GAME_TIME = "fgt";
	// 登录时间所属时段
	public static final String DIMENSION_TIMEPOINT_NUM = "tpn";
	// 两次登录时间间隔
	public static final String DIMENSION_LOGINTIME_INTERTVAL = "lti";
	// 各个时间分布的用户去重数
	public static final String DIMENSION_LOGINTIME_PER_RANGE_USERS = "ltiNum";
	// 玩家等级
	public static final String DIMENSION_PLAYER_LEVEL = "le";
	// 玩家在线天数
	public static final String DIMENSION_PLAYER_DAYS = "ds";
	// 玩家游戏时长
	public static final String DIMENSION_PLAYER_ONLINETIME = "ot";
	// 玩家游戏次数
	public static final String DIMENSION_PLAYER_LoginTimes = "lt";
	// 付费人数
	public static final String DIMENSION_PLAYER_PayTimes = "pt";
	// 付费金额
	public static final String DIMENSION_PLAYER_PayAmount = "pa";

	// 付费方式
	public static final String DIMENSION_PLAYER_PayType = "pmt";

	// sim卡
	public static final String DIMENSION_PLAYER_SIMCODE = "sim";

	// 付费点金额
	public static final String DIMENSION_PLAYER_PayPointAmount = "ppa";
	// 付费点人数
	public static final String DIMENSION_PLAYER_PayPointNum = "ppn";

	// 付费分布：原始货币
	public static final String DIMENSION_PAY_CURRENCY_ORIGINAL = "poc";
	// 付费分布：最终货币
	public static final String DIMENSION_PAY_CURRENCY_FINAL = "pfc";
	// sim卡付费分布
	public static final String DIMENSION_PAY_SIM_AMOUNT = "psa";

	// 周付费人数
	public static final String DIMENSION_WEEK_PayTimes = "wpt";
	// 周付费金额
	public static final String DIMENSION_WEEK_PayAmount = "wpa";
	// 月付费人数
	public static final String DIMENSION_MONTH_PayTimes = "mpt";
	// 月付费金额
	public static final String DIMENSION_MONTH_PayAmount = "mpa";

	// 等级付费人次
	public static final String DIMENSION_PAY_LEVEL_TIMES = "plt";
	// 等级付费金额
	public static final String DIMENSION_PAY_LEVEL_CURRENCY = "plc";

	// 首付统计标识
	// 首付时玩家游戏天数
	// public static final String DIMENSION_FIRST_PAY_GAMEDAYS = "fpgd";
	// 首付时玩家游戏时长
	// public static final String DIMENSION_FIRST_PAY_GAMETIME = "fpgt";
	// 首付时玩家游戏等级
	// public static final String DIMENSION_FIRST_PAY_GAMELEVEL = "fpgl";
	// 首付金额 currency
	public static final String DIMENSION_FIRST_PAY_GAMECURR = "am";

	// 游戏版本升级分布（由低版本升级到高版本）
	public static final String DIMENSION_VERSION_UPDATE_NUM = "vun";

	// ----------- 等级分布统计维度标识 -----------
	// 活跃用户按等级分布人数 Active Palyer Num
	public static final String DIMENSION_LEVEL_PLAYER_NUM = "a1n";
	// 活跃用户当天游戏次数按等级分布 Active Palyer LoginTimes
	public static final String DIMENSION_LEVEL_LOGIN_TIMES = "a1lt";
	// 7 天流失玩家按等级分布人数
	public static final String DIMENSION_LEVEL_L7_LOST_NUM = "L7n";
	// 14 天流失玩家按等级分布人数
	public static final String DIMENSION_LEVEL_L14_LOST_NUM = "L14n";
	// 30 天流失玩家按等级分布人数
	public static final String DIMENSION_LEVEL_L30_LOST_NUM = "L30n";
	// 升级时长
	public static final String DIMENSION_LEVEL_UPGRADE_TIME = "leut";
	// 升级滞停
	public static final String DIMENSION_LEVEL_STOP = "lest";
	// 升级时长区间分布
	public static final String DIMENSION_LEVEL_TIME_DIST = "letd";

	// 关卡统计维度
	// 人数
	public static final String DIMENSION_GUANKA_PLAYER_NUM = "level_player_num";
	public static final String DIMENSION_GUANKA_PLAYER_NUM_total = "total";
	// 次数
	public static final String DIMENSION_GUANKA_PLAY_TIMES = "level_play_times";
	public static final String DIMENSION_GUANKA_PLAY_TIMES_total = "total";
	public static final String DIMENSION_GUANKA_PLAY_TIMES_succ = "succ";
	public static final String DIMENSION_GUANKA_PLAY_TIMES_fail = "fail";
	public static final String DIMENSION_GUANKA_PLAY_TIMES_exit = "exit";
	// 时长
	public static final String DIMENSION_GUANKA_DUARTION = "level_runtime";
	public static final String DIMENSION_GUANKA_DUARTION_total = "total";
	public static final String DIMENSION_GUANKA_DUARTION_succ = "succ";
	public static final String DIMENSION_GUANKA_DUARTION_fail = "fail";

	// 关卡失败原因人数统计
	public static final String DIMENSION_GUANKA_FAIL_REASON_NUM = "level_fail_resaon_num";
	// 关卡失败原因次数统计
	public static final String DIMENSION_GUANKA_FAIL_REASON_TIMES = "level_fail_resaon_times";
	// 付费
	public static final String DIMENSION_GUANKA_PAYMENT = "level_pay";
	public static final String DIMENSION_GUANKA_PAYMENT_times = "times";
	public static final String DIMENSION_GUANKA_PAYMENT_amount = "amount";
	// 道具购买
	public static final String DIMENSION_GUANKA_ITEM_BUY = "level_item_buy";
	// 道具消耗
	public static final String DIMENSION_GUANKA_ITEM_USE = "level_item_consume";

	// added by ivan --start
	// 房间局数分布
	public static final String DIMENSION_ROOM_PLAYTIMES = "rp";
	public static final String DIMENSION_ROOM_COIN_GAIN = "rcg";
	public static final String DIMENSION_ROOM_COIN_LOST = "rcl";
	// 房间用户类型玩家总数 add by mikefeng 20140919
	public static final String DIMENSION_ROOM_USERTYPE_ACCOUNT_SUM = "ruas";
	public static final String DIMENSION_COIN = "coin";
	public static final String DIMENSION_GAIN = "coing";
	public static final String DIMENSION_LOST = "coinl";
	public static final String DIMENSION_RETAIN = "coinr";

	// added by ivan --end

	// Added at 20140617：虚拟币需分种类统计，加入默认种类
	public static final String DEFAULT_COIN_TYPE = "coin";

	// added by mikefeng 2014-08-19 -- start
	public static final String SUFFIX_COIN_GAIN_LOST_USERDURE = "COIN_GAIN_LOST_USERDURE";
	// added by mikefeng 2014-08-19 -- end

	// added by mikefeng 2014-08-19 -- start
	// 虚拟币消费用户
	public static final String DIMENSION_COIN_LOST_USER_NUM = "clun";
	// added by mikefeng 2014-08-19 -- end

	// 升级事件 id，用于从事件中把升级事件抽取出来
	public static final String EVENT_ID_LEVEL_UP = "_DESelf_Level";
	// 事件次数统计，属性 ID
	public static final String EVENT_ATTR_ID_COUNT = "_DESelf_Count";
	public static final String EVENT_NETWORK_TRAFFICE = "DESelf_APP_TRAFFIC";

	// 自定义事件 ID
	// 存量金币变化
	public static final String DESelf_Coin_Num = "DESelf_Coin_Num";
	// 棋牌进行一局
	public static final String DESelf_CG_PlayCards = "DESelf_CG_PlayCards";
	// 金币消耗
	public static final String DESelf_Coin_Lost = "DESelf_Coin_Lost";
	// 金币获得
	public static final String DESelf_Coin_Gain = "DESelf_Coin_Gain";
	// 房间内金币消耗
	public static final String DESelf_CG_Lost = "DESelf_CG_Lost";
	// 房间内金币获得
	public static final String DESelf_CG_Gain = "DESelf_CG_Gain";

	// --------------- 任务与道具
	// 道具购买
	public static final String DESelf_ItemBuy = "DESelf_ItemBuy";
	// 道具获得
	public static final String DESelf_ItemGet = "DESelf_ItemGet";
	// 道具消耗
	public static final String DESelf_ItemUse = "DESelf_ItemUse";
	// 任务开始
	public static final String DESelf_TaskBegin = "DESelf_TaskBegin";
	// 任务结束
	public static final String DESelf_TaskEnd = "DESelf_TaskEnd";
	// 关卡开始
	public static final String DESelf_LevelsBegin = "DESelf_LevelsBegin";
	// 关卡结束
	public static final String DESelf_LevelsEnd = "DESelf_LevelsEnd";

	// -----------------TCL 相关自定义事件-----------------
	// 页面浏览
	public static final String DESelf_APP_NAVIGATION = "DESelf_APP_NAVIGATION";
	// 浏览功能
	public static final String DESelf_APP_MODULE = "DESelf_APP_MODULE";
	// 关键字搜索
	public static final String DESelf_APP_SOURCE = "DESelf_APP_SOURCE";
	// APP 查看
	public static final String DESelf_APP_VIEW = "DESelf_APP_VIEW";
	// APP 下载(关键字搜索)
	public static final String DESelf_APP_DOWNLOAD = "DESelf_APP_DOWNLOAD";

	/*************** 渠道版相关自定义事件 *****************/
	/** 资源位曝光 */
	public static final String DESELF_CHANNEL_RL_SHOW = "_DESelf_Channel_RL_Show";
	/** 资源位点击 */
	public static final String DESELF_CHANNEL_RL_CLICK = "_DESelf_Channel_RL_Click";
	/** 资源搜索 */
	public static final String DESELF_CHANNEL_RES_SEARCH = "_DESelf_Channel_Res_Search";
	/** 资源下载 */
	public static final String DESELF_CHANNEL_RES_DOWNLOAD = "_DESelf_Channel_Res_Download";
	/** 资源下载成功 */
	public static final String DESELF_CHANNEL_DOWNLOAD_RESULT = "_DESelf_Channel_Download_Result";
	/** 应用列表 */
	public static final String DESELF_APP_LIST = "_DESelf_App_List";
	/** 应用安装 */
	public static final String DESELF_APP_INSTALL = "_DESelf_App_Install";
	/** 应用卸载 */
	public static final String DESELF_APP_UNINSTALL = "_DESelf_App_Uninstall";
	/** 应用启动-在线 */
	public static final String DESELF_APP_LAUNCH = "_DESelf_App_Launch";
	/** 错误日志 */
	public static final String DESELF_ERROR_REPORT = "DESelf_ErrorReport";
	/** 页面路径 */
	public static final String DESELF_CHANNEL_PAGE_NAVIGATION = "DESelf_Channel_PageNavigation";

	// ----------- 首充、二充、三充 标识
	// public static final String DIMENSION_PAY_TIME_INTERVAL = "pti";
	public static final String DIMENSION_PAY_TIME_INTERVAL_1 = "pti1";
	public static final String DIMENSION_PAY_TIME_INTERVAL_2 = "pti2";
	public static final String DIMENSION_PAY_TIME_INTERVAL_3 = "pti3";

	// ----------- 任务道具分布维度标识
	// 道具购买人数
	public static final String DIMENSION_ITEM_BUY_PLAYERNUM = "Item_Buy_PlayerNum";
	// 某种虚拟币购买道具的数量
	public static final String DIMENSION_ITEM_BUY_NUM = "Item_Buy_Num";
	// 某种虚拟币购买道具的总额度
	public static final String DIMENSION_ITEM_BUY_COIN_VALUE = "Item_Buy_Coin_Value";
	// 道具获得的人数总和
	public static final String DIMENSION_ITEM_SYS_OUTPUT_PLAYERNUM = "Item_Sys_Output_PlayerNum";
	public static final String DIMENSION_ITEM_SYS_OUTPUT_TOTALPLAYERNUM = "Item_Sys_Output_TotalPlayerNum";
	// 道具获取数量在获取方式上分布
	public static final String DIMENSION_ITEM_SYS_OUTPUT_NUM = "Item_Sys_Output_Num";
	// 道具消耗数量在消耗方式上分布
	public static final String DIMENSION_ITEM_SYS_CONSUME_NUM = "Item_Sys_Consume_Num";

	// 任务开始在等级上的分布
	public static final String DIMENSION_TASK_BEGIN_LEVEL = "tble";
	// 任务完成在等级上的分布
	public static final String DIMENSION_TASK_FINISH_LEVEL = "tfle";
	// 任务失败在原因的分布
	public static final String DIMENSION_TASK_FAILED_REASON = "tfre";

	// 鲸鱼玩家付费在平台、版本、渠道、区服维度的统计类型
	public static final String DIMENSION_PAY_ON_VERSION = "v";
	public static final String DIMENSION_PAY_ON_CHANNEL = "ch";
	public static final String DIMENSION_PAY_ON_GAMESERVER = "gs";

	// sim卡 分布扩展字段的几个key add by mikefeng 20141020
	public static final String INVOKELOG = "INVOKELOG";
	public static final String SIM_OPERATOR_ISO = "SIM_OPERATOR_ISO";
	public static final String SIM_OPERATOR = "SIM_OPERATOR";
	public static final String IMEI = "IMEI";
	public static final String REPORTMODE = "REPORTMODE";
	public static final String WIFIMAC = "WIFIMAC";
	public static final String IDFA = "IDFA";
	public static final String ROLEID = "RoleId";
	public static final String ROLENAME = "RoleName";
	public static final String IMSI = "IMSI";

	// HTML5 日志最后一个扩展字段中的几个 key
	public static final String H5_APP = "H5_APP";
	public static final String H5_DOMAIN = "H5_DOMAIN";
	public static final String H5_REF = "H5_REF";
	public static final String H5_CRTIME = "H5_CRTIME";
	public static final String H5_PARENT = "H5_PARENT";
	// HTML5 Suffix
	public static final String SUFFIX_H5_ONLINEDAY_FOR_PLAYER = "H5_ONLINEDAY_FOR_PLAYER";
	public static final String SUFFIX_H5_ONLINEDAY_FOR_APP = "H5_ONLINEDAY_FOR_APP";
	public static final String SUFFIX_H5_PV_FOR_PLAYER = "H5_PV_FOR_PLAYER";
	public static final String SUFFIX_H5_PV_FOR_APP = "H5_PV_FOR_APP";
	public static final String SUFFIX_H5_VIRUS_SPREAD_FOR_APP = "H5_VIRUS_SPREAD_FOR_APP";
	public static final String SUFFIX_H5_VIRUS_SPREAD_FOR_PARENT = "H5_VIRUS_SPREAD_FOR_PARENT";
	public static final String SUFFIX_H5_VIRUS_SPREAD_FOR_PLAYER = "H5_VIRUS_SPREAD_FOR_PLAYER";
	public static final String SUFFIX_H5_VIRUS_SPREAD_TOP100 = "H5_VIRUS_SPREAD_TOP100";
	public static final String SUFFIX_H5_VIRUS_SPREAD_KFACTOR = "H5_VIRUS_SPREAD_KFACTOR";

	public static final String SUFFIX_H5_NEWADD_DAY_SUM = "H5_NEWADD_DAY_SUM";
	public static final String SUFFIX_H5_30DAY_RETAIN_SUM = "H5_30DAY_RETAIN_SUM";

	// HTML5 new Suffix
	public static final String SUFFIX_H5_NEW_USERINFO_DAY = "H5_NEW_USERINFO_DAY";
	public static final String SUFFIX_H5_NEW_ONLINEDAY = "H5_NEW_ONLINEDAY";
	public static final String SUFFIX_H5_NEW_PV = "H5_NEW_PV";
	public static final String SUFFIX_H5_NEW_STAT_ONLINEDAY_APP = "H5_NEW_STAT_ONLINEDAY_APP";
	public static final String SUFFIX_H5_NEW_ONLINEDAY_FOR_APP = "H5_NEW_ONLINEDAY_FOR_APP";
	public static final String SUFFIX_H5_NEW_STAT_PV_APP = "H5_NEW_STAT_PV_APP";
	public static final String SUFFIX_H5_NEW_PV_FOR_APP = "H5_NEW_PV_FOR_APP";
	public static final String SUFFIX_H5_NEW_VIRUS_SPREAD = "H5_NEW_VIRUS_SPREAD";
	public static final String SUFFIX_H5_NEW_VIRUS_SPREAD_FOR_APP = "H5_NEW_VIRUS_SPREAD_FOR_APP";
	public static final String SUFFIX_H5_NEW_VIRUS_SPREAD_KFACTOR = "H5_NEW_VIRUS_SPREAD_KFACTOR";
	public static final String SUFFIX_H5_NEW_VIRUS_SPREAD_TOP100 = "H5_NEW_VIRUS_SPREAD_TOP100";
	public static final String SUFFIX_H5_USER_LOST_FUNNEL = "H5_USER_LOST_FUNNEL";
	public static final String SUFFIX_H5_USER_LOST_FUNNEL_APP = "H5_USER_LOST_FUNNEL_APP";
	public static final String SUFFIX_H5_NEW_ADD_ONLINE_PAY_PLAYER = "H5_NEW_ADD_ONLINE_PAY_PLAYER";
	public static final String SUFFIX_H5_NEW_PLAYER_ONLINE_INFO = "H5_NEW_PLAYER_ONLINE_INFO";
	public static final String SUFFIX_H5_NEW_LAYOUT_ON_DEVICE = "H5_NEW_LAYOUT_ON_DEVICE";
	public static final String SUFFIX_H5_NEW_ONLINE_PAY = "H5_NEW_ONLINE_PAY";

	// h5 event
	public static final String SUFFIX_H5_EVENT_STAT = "H5_EVENT_STAT";
	public static final String SUFFIX_H5_EVENT_ATTR_STAT = "H5_EVENT_ATTR_STAT";

	// 输出ad广告相关数据
	public static final String SUFFIX_AD_LABEL_INFO = "AD_LABEL_INFO";

	// 滚服相关后缀
	public static final String SUFFIX_GS_ROLL_PLAYER_INFO = "GS_ROLL_PLAYER_INFO";
	public static final String SUFFIX_GS_ROLL_30DAY_RETAIN = "GS_ROLL_30DAY_RETAIN";
	public static final String SUFFIX_GS_ROLL_PLAYER_NUM = "GS_ROLL_PLAYER_NUM";
	public static final String SUFFIX_GS_ROLL_PAYMENT = "GS_ROLL_PAYMENT";
	public static final String SUFFIX_GS_ROLL_30DAY_RETAIN_SUM = "GS_ROLL_30DAY_RETAIN_SUM";
	// 插件后缀
	public static final String SUFFIX_PLUGIN_PaymentReport = "PLUGIN_PaymentReport";
	public static final String SUFFIX_PLUGIN_CoinReport = "PLUGIN_CoinReport";
	public static final String SUFFIX_PLUGIN_ItemReport = "PLUGIN_ItemReport";
	// 滚服每天付费金额
	public static final String DIMENSION_TODAY_PAY_CUR = "payCur";
	// 滚服每天付费人数
	public static final String DIMENSION_TODAY_PAY_NUM = "payNum";
	// 滚服首日付费金额
	public static final String DIMENSION_FIRSTDAY_PAY_CUR = "fdpCur";
	// 滚服首日付费人数
	public static final String DIMENSION_FIRSTDAY_PAY_NUM = "fdpNum";
	// 滚服首次付费金额
	public static final String DIMENSION_FIRSTTIME_PAY_CUR = "ftpCur";
	// 滚服首次付费人数
	public static final String DIMENSION_FIRSTTIME_PAY_NUM = "ftpNum";

	// 用户流失类型 新增活跃付费
	// 玩家流失--- L7/14/30
	// 付费玩家流失--- PL7/14/30
	// 玩家回流 --- B7/14/30
	// 付费玩家回流 --- PB7/14/30
	// 新增玩家留存 --- S1/7/14/30 , 次日，7日，14日，30日留存
	public enum UserLostType {
		// 流失
		NewUserLost1("NL1"), NewUserLost3("NL3"), NewUserLost7("NL7"), NewUserLost14("NL14"), NewUserLost30("NL30"), UserLost1(
				"L1"), UserLost3("L3"), UserLost7("L7"), UserLost14("L14"), UserLost30("L30"), PayUserLost1("PL1"), PayUserLost3(
				"PL3"), PayUserLost7("PL7"), PayUserLost14("PL14"), PayUserLost30("PL30"),
		// Added at 20140904 曾经付过费的玩家流失
		EverPayUserLost1("EPL1"), EverPayUserLost3("EPL3"), EverPayUserLost7("EPL7"), EverPayUserLost14("EPL14"), EverPayUserLost30(
				"EPL30"),

		// 回流
		NewUserBack7("NB7"), NewUserBack14("NB14"), NewUserBack30("NB30"), UserBack7("B7"), UserBack14("B14"), UserBack30(
				"B30"), PayUserBack7("PB7"), PayUserBack14("PB14"), PayUserBack30("PB30"),
		// Added at 20140904 曾经付过费的玩家流失
		EverPayUserBack7("EPB7"), EverPayUserBack14("EPB14"), EverPayUserBack30("EPB30"),

		// 留存
		NewUserStay1("NS1"), NewUserStay7("NS7"), NewUserStay14("NS14"), NewUserStay30("NS30"), PayUserStay1("PS1"), PayUserStay7(
				"PS7"), PayUserStay14("PS14"), PayUserStay30("PS30"), UserStay1("S1"), UserStay7("S7"), UserStay14(
				"S14"), UserStay30("S30");

		public String value;

		UserLostType(String value) {
			this.value = value;
		}
	}

	// ---------------------add by ivan 2014-06-06
	/**
	 * 网络流量
	 */
	public static final String NETWORK_TRAFFIC = "nwtf";
	public static final String APPLIST_RINGTONE = "ringtone";
	public static final String SUFFIX_TRAFFIC_HOUR = "TRAFFIC_HOUR";
	public static final String SUFFIX_TRAFFIC_DAY = "TRAFFIC_DAY";
	public static final String SUFFIX_DOWNLOAD_TIMES = "DOWN_LOAD_TIMES";
	public static final String SUFFIX_DOWNLOAD_SUCCESS_TIMES = "DOWN_LOAD_SUCCESS_TIMES";
	public static final String SUFFIX_DOWNLOAD_COST = "DOWN_LOAD_COST";
	public static final String SUFFIX_DOWNLOAD_FAIL_REASON = "DL_FAIL";
	public static final String SUFFIX_DOWNLOAD_INTERRUPT_REASON = "DL_PAUSE";
	public static final String SUFFIX_APPLIST_RINGTONE = "APPLIST_RINGTONG";
	public static final String SUFFIX_APPLIST_APP = "APPLIST_APP";
	public static final String SUFFIX_APPLIST_ROLLING_DAY = "APPLIST_ROLLING_DAY";
	public static final String NETWORK_TRAFFIC_UP = "UP";
	public static final String NETWORK_TRAFFIC_DOWN = "DOWN";
	public static final String NETTYPE_ALL_NT = "100";
	public static final String DESELF_APP_DOWNLOAD_START = "DESelf_APP_DOWNLOAD_START";
	public static final String DESELF_APP_DOWNLOAD_COMPLETE = "DESelf_APP_DOWNLOAD_COMPLETE";
	public static final String DESELF_APP_DOWNLOAD_INTERRUPT = "DESelf_APP_DOWNLOAD_INTERRUPT";

	// sdk 版本升级通知 add by mikefeng
	public static final String SUFFIX_SDK_VERSION_UPDATE = "SDK_VERSION_UPDATE";
	// 房间用户类型玩家数 add by mikefeng
	public static final String SUFFIX_ROOM_USER_ACCOUNT = "ROOM_USER_ACCOUNT";

	// 付费方式分布 add by mikefeng 20141024
	public static final String SUFFIX_PAYMENT_PAYTYPE_DAY = "PAYMENT_PAYTYPE_DAY";
	// 付费点
	public static final String SUFFIX_PAYMENT_POINT_DAY = "PAYMENT_POINT_DAY";
	// 付费点 - 金额
	public static final String SUFFIX_PAYMENT_POINT_AMOUNT_DAY = "PAYMENT_POINT_AMOUNT_DAY";
	// 付费点 - 人数
	public static final String SUFFIX_PAYMENT_POINT_NUM_DAY = "PAYMENT_POINT_NUM_DAY";
	// 付费货币
	public static final String SUFFIX_PAYMENT_CURRENCY_DAY = "PAYMENT_CURRENCY_DAY";
	// 付费货币金额- 原始货币
	public static final String SUFFIX_PAYMENT_CURRENCY_ORIGINAL_DAY = "PAYMENT_CURRENCY_ORIGINAL_DAY";
	// 付费货币 金额- 最终金额
	public static final String SUFFIX_PAYMENT_CURRENCY_FINAL_DAY = "PAYMENT_CURRENCY_FINAL_DAY";
	// 付费 - SIM卡
	public static final String SUFFIX_PAYMENT_SIMCODE_DAY = "PAYMENT_SIMCODE_DAY";

	// 种子k系数top100
	public static final int K_TYPE_FACTORY = 0;
	// 活跃k系数top100
	public static final int K_TYPE_ONLINE = 1;
	// 滚存H5 suffix
	public static final String SUFFIX_H5_USERROLLING = "H5_USERROLLING";
	// H5 活跃玩家for分布
	public static final String SUFFIX_H5_ONLINEDAY_FOR_PLAYER_DEVICE = "H5_ONLINEDAY_FOR_PLAYER_DEVICE";
	public static final String SUFFIX_H5_LAYOUT_ON_DEVICE = "H5_LAYOUT_ON_DEVICE";

	// 自定义事件 ID
	// 错误日志
	public static final String DESelf_UserDefined_ErrorReport = "DESelf_UserDefined_ErrorReport";
	public static final String DESelf_UserDefined_ErrorReport_2 = "DESelf_UserDefined_ErrorReport_2";
	// 新版用户自定义错误sdk对errorTitle和errorDetail做了URLEncoder
	public static final String DESelf_UserDefined_ErrorReport_3 = "DESelf_UserDefined_ErrorReport_3";
	// 系统错误日志
	public static final String ErrorReport = "ErrorReport";
	// errorreport detail suffix
	public static final String SUFFIX_ERROR_REPORT_DETAIL_SYS = "ERROR_REPORT_DETAIL_SYS";
	public static final String SUFFIX_ERROR_REPORT_DIST_SYS = "ERROR_REPORT_DIST_SYS";
	public static final String SUFFIX_ERROR_REPORT_DETAIL_USER = "ERROR_REPORT_DETAIL_USER";
	public static final String SUFFIX_ERROR_REPORT_DIST_USER = "ERROR_REPORT_DIST_USER";

	// 统计玩家新增日期、历史充值 add by mikefeng 20141113
	public static final String SUFFIX_PLAYER_NEWADD_AND_PAY = "PLAYER_NEWADD_AND_PAY";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_CNT_7 = "ITEM_BUY_FOR_PLAYER_CNT_7";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_VIRCUR_7 = "ITEM_BUY_FOR_PLAYER_VIRCUR_7";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_CNT_30 = "ITEM_BUY_FOR_PLAYER_CNT_30";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_VIRCUR_30 = "ITEM_BUY_FOR_PLAYER_VIRCUR_30";

	public static final String SUFFIX_ITEM_BUY_PLAYER_CNT_SUM_7 = "ITEM_BUY_PLAYER_CNT_SUM_7";
	public static final String SUFFIX_ITEM_BUY_PLAYER_CNT_SUM_30 = "ITEM_BUY_PLAYER_CNT_SUM_30";

	public static final String SUFFIX_ITEM_GET_PLAYER_CNT_SUM_7 = "ITEM_GET_PLAYER_CNT_SUM_7";
	public static final String SUFFIX_ITEM_GET_PLAYER_CNT_SUM_30 = "ITEM_GET_PLAYER_CNT_SUM_30";
	public static final String SUFFIX_ITEM_USE_PLAYER_CNT_SUM_7 = "ITEM_USE_PLAYER_CNT_SUM_7";
	public static final String SUFFIX_ITEM_USE_PLAYER_CNT_SUM_30 = "ITEM_USE_PLAYER_CNT_SUM_30";

	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_GETCNT_7 = "ITEM_BUY_FOR_PLAYER_GETCNT_7";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_USECNT_7 = "ITEM_BUY_FOR_PLAYER_USECNT_7";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_GETCNT_30 = "ITEM_BUY_FOR_PLAYER_GETCNT_30";
	public static final String SUFFIX_ITEM_BUY_FOR_PLAYER_USECNT_30 = "ITEM_BUY_FOR_PLAYER_USECNT_30";

	public static final String SUFFIX_ITEM_BUY_SUM_7 = "ITEM_BUY_SUM_7";
	public static final String SUFFIX_ITEM_BUY_SUM_30 = "ITEM_BUY_SUM_30";

	public static final String SUFFIX_ITEM_GET_SUM_7 = "ITEM_GET_SUM_7";
	public static final String SUFFIX_ITEM_GET_SUM_30 = "ITEM_GET_SUM_30";
	public static final String SUFFIX_ITEM_USE_SUM_7 = "ITEM_USE_SUM_7";
	public static final String SUFFIX_ITEM_USE_SUM_30 = "ITEM_USE_SUM_30";

	public static final String SUFFIX_PLAYER_INFO_FOR_EXT = "PLAYER_INFO_FOR_EXT";

	// 把新增日期和首付日期记录到 HBase
	public static final String SUFFIX_NEWADD_DAY_4_HBASE = "NEWADD_DAY_4_HBASE";

	// 标签统计
	public static final String TAG_DEFAULT_ID = "DE_DF_PID";
	public static final String EVENT_ID_ADD_TAG = "DESelf_addTag";
	public static final String EVENT_ID_RM_TAG = "DESelf_removeTag";

	public static final String SUFFIX_TAG_ROLLING_DAY = "TAG_ROLLING_DAY";
	// public static final String SUFFIX_TAG_INFO_DAY = "TAG_INFO_DAY";
	public static final String SUFFIX_TAG_STAY_ADD_RM = "TAG_STAY_ADD_RM";
	public static final String SUFFIX_TAG_STAY_ADD_RM_SUM = "TAG_STAY_ADD_RM_SUM";
	public static final String SUFFIX_TAG_ONLINE_DAY = "TAG_ONLINE_DAY";
	public static final String SUFFIX_TAG_PAYMENT_DAY = "TAG_PAYMENT_DAY";
	public static final String SUFFIX_TAG_OL_PAY_SUM = "TAG_OL_PAY_SUM";
	public static final String SUFFIX_TAG_LEVEL_PLAYER_NUM = "TAG_LEVEL_PLAYER_NUM";
	public static final String SUFFIX_TAG_PLAYER_RETAIN = "TAG_PLAYER_RETAIN";
	public static final String SUFFIX_TAG_PLAYER_RETAIN_SUM = "TAG_PLAYER_RETAIN_SUM";
	public static final String SUFFIX_LOST_PLAYER_FOR_TAG = "LOST_PLAYER_FOR_TAG";
	public static final String SUFFIX_TAG_PLAYER_LOST = "TAG_PLAYER_LOST";
	public static final String SUFFIX_TAG_PLAYER_LOST_SUM = "TAG_PLAYER_LOST_SUM";
	public static final String SUFFIX_TAG_PLAYER_LOST_LEVEL = "TAG_PLAYER_LOST_LEVEL";

	/******************* 渠道版相关MR输出后缀 *******************/

	/** 资源位曝光 */
	public static final String SUFFIX_CHANNEL_RL_SHOW = "CHANNEL_RL_SHOW";
	/** 资源位点击 */
	public static final String SUFFIX_CHANNEL_RL_CLICK = "CHANNEL_RL_CLICK";
	/** 资源搜索 */
	public static final String SUFFIX_CHANNEL_RES_SEARCH = "CHANNEL_RES_SEARCH";
	/** 资源下载 */
	public static final String SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL = "CHANNEL_RES_DOWNLOAD_TOTAL";
	/** 资源位下载 */
	public static final String SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL = "CHANNEL_RL_DOWNLOAD_TOTAL";
	/** 关键词下载 */
	public static final String SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL = "CHANNEL_KW_DOWNLOAD_TOTAL";
	/** 资源下载 -按资源位分布 = 资源位下载-按资源分布 */
	public static final String SUFFIX_CHANNEL_RES_DOWNLOAD_RL = "CHANNEL_RES_DOWNLOAD_RL";
	/** 资源下载 -按关键字分布 = 搜索字下载-资资源分布 */
	public static final String SUFFIX_CHANNEL_RES_DOWNLOAD_KW = "CHANNEL_RES_DOWNLOAD_KW";
	/** 资源下载成功 */
	public static final String SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT = "CHANNEL_RES_DOWNLOAD_RESULT";
	/** 应用列表 */
	public static final String SUFFIX_APP_LIST = "APP_LIST";
	/** 应用在线统计：启动，在线时长，安装，卸载等 */
	public static final String SUFFIX_APP_USE_HABIT = "APP_USE_HABIT";
	/** 应用在线统计：启动，在线时长等 */
	public static final String SUFFIX_APP_LAUNCH = "APP_LAUNCH";
	/** 应用安装 */
	public static final String SUFFIX_APP_INSTALL = "APP_INSTALL";
	/** 应用卸载 */
	public static final String SUFFIX_APP_UNINSTALL = "APP_UNINSTALL";
	/** 错误日志 - 详情 */
	public static final String SUFFIX_CHANNEL_ERROR_REPORT_DETAIL = "CHANNEL_ERROR_REPORT_DETAIL";
	/** 错误日志 - 分布 */
	public static final String SUFFIX_CHANNEL_ERROR_REPORT_DIST = "CHANNEL_ERROR_REPORT_DIST";
	/** 页面访问树 */
	public static final String SUFFIX_CHANNEL_PAGETREE_4_LOGINTIME = "CHANNEL_PAGETREE_4_LOGINTIME";
	/** 页面跳转 */
	public static final String SUFFIX_CHANNEL_PAGETREE_SUM = "CHANNEL_PAGETREE_SUM";
	/** 事件统计 */
	public static final String SUFFIX_CHANNEL_EVENT_STAT = "CHANNEL_EVENT_STAT";
	/** 事件属性统计 */
	public static final String SUFFIX_CHANNEL_EVENT_ATTR_STAT = "CHANNEL_EVENT_ATTR_STAT";

	// 激活日志
	public static final String SUFFIX_CHANNEL_USERINFO = "CHANNEL_USERINFO";
	// 在线日志
	public static final String SUFFIX_CHANNEL_ONLINE = "CHANNEL_ONLINE";
	// 滚存日志
	public static final String SUFFIX_CHANNEL_ROLLING = "CHANNEL_ROLLING";
	// 滚存输出在线情况
	public static final String SUFFIX_CHANNEL_ONLINE_INFO = "CHANNEL_ONLINE_INFO";
	// 留存
	public static final String SUFFIX_CHANNEL_RETAIN = "CHANNEL_RETAIN";
	// 新鲜度
	public static final String SUFFIX_CHANNEL_FRESH_USER = "CHANNEL_FRESH_USER";
	// 在线人数、在线时长、登录次数
	public static final String SUFFIX_CHANNEL_ONLINE_SUM = "CHANNEL_ONLINE_SUM";
	public static final String SUFFIX_CHANNEL_LAYOUT_EQUIP = "CHANNEL_LAYOUT_EQUIP"; // 设备分布
	public static final String SUFFIX_CHANNEL_LAYOUT_ONLINE = "CHANNEL_LAYOUT_ONLINE"; // 在线分布
	// 用户下载汇总
	public static final String SUFFIX_CHANNEL_D_FOR_PLAYER = "CHANNEL_D_FOR_PLAYER";
	// 数据中心
	public static final String SUFFIX_CHANNEL_GAME_CENTER = "CHANNEL_GAME_CENTER";
	// 下载分布
	public static final String SUFFIX_CHANNEL_DLD_TIMES_LAYOUT = "CHANNEL_DLD_TIMES_LAYOUT";
	public static final String SUFFIX_CHANNEL_DLD_NUMS_LAYOUT = "CHANNEL_DLD_NUMS_LAYOUT";
	// 新增活跃玩家下载人数
	public static final String SUFFIX_CHANNEL_PLAYER_DLD_NUM = "CHANNEL_PLAYER_DLD_NUM";

	// 周------------
	// 由天滚存输出玩家本周游戏行为情况
	public static final String SUFFIX_CHANNEL_WEEK_INFO = "CHANNEL_WEEK_INFO";
	public static final String SUFFIX_CHANNEL_WEEK_HABITS = "CHANNEL_WEEK_HABITS";
	// 由周滚存输出玩家历史游戏行为情况
	public static final String SUFFIX_CHANNEL_ROLLING_WEEK = "CHANNEL_ROLLING_WEEK";
	// 由周滚存输出周留存情况
	public static final String SUFFIX_CHANNEL_WEEK_RETAIN = "CHANNEL_WEEK_RETAIN";
	// 月------------
	// 由天滚存输出玩家本月游戏行为情况
	public static final String SUFFIX_CHANNEL_MONTH_INFO = "CHANNEL_MONTH_INFO";
	public static final String SUFFIX_CHANNEL_MONTH_HABITS = "CHANNEL_MONTH_HABITS";
	// 由月滚存输出玩家历史游戏行为情况
	public static final String SUFFIX_CHANNEL_ROLLING_MONTH = "CHANNEL_ROLLING_MONTH";
	// 由周滚存输出月留存情况
	public static final String SUFFIX_CHANNEL_MONTH_RETAIN = "CHANNEL_MONTH_RETAIN";

	// 分布表维度
	/** 机型分布 */
	public static final int DIM_TYPE_BRAND_DIS = 1;
	/** 下载资源分布 */
	public static final int DIM_TYPE_DLD_RESOURCE_DIS = 2;
	/** 下载次数分布 */
	public static final int DIM_TYPE_DLD_TIMES_DIS = 3;
	/** 启动时间点分布 */
	public static final int DIM_TYPE_LOGIN_HOUR_DIS = 4;
	/** 启动次数分布 */
	public static final int DIM_TYPE_LOGIN_TIMES_DIS = 5;
	/** 联网方式分布 */
	public static final int DIM_TYPE_NET_TYPE_DIS = 6;
	/** 宽带运营商分布 */
	public static final int DIM_TYPE_BROAD_OPER_DIS = 7;
	/** 移动运营商分布 */
	public static final int DIM_TYPE_MOBILE_OPER_DIS = 8;
	/** 玩家城市分布 */
	public static final int DIM_TYPE_CITY_NUM_DIS = 9;

	/****************************** 数据仓库MR输出后缀 ******************************/
	/** 在线明细 */
	public static final String SUFFIX_WAREHOUSE_ONLINE_DETAIL = "WAREHOUSE_ONLINE_DETAIL";
	/** 在线按天 */
	public static final String SUFFIX_WAREHOUSE_ONLINE_DAY = "WAREHOUSE_ONLINE_DAY";
	/** ACCOUNT滚存 */
	public static final String SUFFIX_WAREHOUSE_ACCOUNT_ROLLING = "WAREHOUSE_ACCOUNT_ROLLING";
	/** 设备滚存 */
	public static final String SUFFIX_WAREHOUSE_DEVICE_ROLLING = "WAREHOUSE_DEVICE_ROLLING";
	/** IMSI滚存 */
	public static final String SUFFIX_WAREHOUSE_IMSI_ROLLING = "WAREHOUSE_IMSI_ROLLING";
	/** ACCOUNT登录 */
	public static final String SUFFIX_WAREHOUSE_ACCOUNT_LOGIN = "WAREHOUSE_ACCOUNT_LOGIN";
	/** UID活跃地区 */
	public static final String SUFFIX_WAREHOUSE_UID_LOCATION = "WAREHOUSE_UID_LOCATION";
	/** UID 小时分布 */
	public static final String SUFFIX_WAREHOUSE_UID_HOUR = "WAREHOUSE_UID_HOUR";
	/** UID+APPID 小时分布 */
	public static final String SUFFIX_WAREHOUSE_UIDAPP_HOUR = "WAREHOUSE_UIDAPP_HOUR";
	/** APPID 小时分布 */
	public static final String SUFFIX_WAREHOUSE_APP_HOUR = "WAREHOUSE_APP_HOUR";
	/** UID 滚存 */
	public static final String SUFFIX_WAREHOUSE_UID_ROLLING = "WAREHOUSE_UID_ROLLING";
	/** UID+APPID 滚存 */
	public static final String SUFFIX_WAREHOUSE_UIDAPP_ROLLING = "WAREHOUSE_UIDAPP_ROLLING";
	/** APPID 滚存 */
	public static final String SUFFIX_WAREHOUSE_APP_ROLLING = "WAREHOUSE_APP_ROLLING";
	/** UID偏好统计 */
	public static final String SUFFIX_WAREHOUSE_UID_APPHABIT = "WAREHOUSE_UID_APPHABIT";

	/** UID 天统计 */
	public static final String SUFFIX_WAREHOUSE_UID_STAT_DAY = "WAREHOUSE_UID_STAT_DAY";
	/** UID+APPID 天统计 */
	public static final String SUFFIX_WAREHOUSE_UIDAPP_STAT_DAY = "WAREHOUSE_UIDAPP_STAT_DAY";
	
	/** 在线 */
	public static final String SUFFIX_WAREHOUSE_ONLINE = "WAREHOUSE_ONLINE";
	/** 付费明细 */
	public static final String SUFFIX_WAREHOUSE_PAYMENT_DETAIL = "WAREHOUSE_PAYMENT_DETAIL";
	/** 付费按天 */
	public static final String SUFFIX_WAREHOUSE_PAYMENT_DAY = "WAREHOUSE_PAYMENT_DAY";
	/** 付费 */
	public static final String SUFFIX_WAREHOUSE_PAYMENT = "WAREHOUSE_PAYMENT";
	/** 标识码 */
	public static final String SUFFIX_WAREHOUSE_UID = "WAREHOUSE_UID";
	/** 标识码滚存 */
	// public static final String SUFFIX_WAREHOUSE_UID_ROLLING = "WAREHOUSE_UID_ROLLING";
	/** 应用列表 */
	public static final String SUFFIX_WAREHOUSE_APPLIST = "WAREHOUSE_APPLIST";
	/** 应用列表滚存 */
	public static final String SUFFIX_WAREHOUSE_APPLIST_ROLLING = "WAREHOUSE_APPLIST_ROLLING";
	/** 应用详情 */
	public static final String SUFFIX_WAREHOUSE_APPDETAIL = "WAREHOUSE_APPDETAIL";
	/** 应用详情滚存 */
	public static final String SUFFIX_WAREHOUSE_APPDETAIL_ROLLING = "WAREHOUSE_APPDETAIL_ROLLING";
	/** 数据清理 */
	public static final String SUFFIX_WAREHOUSE_CLEAN = "WAREHOUSE_CLEAN";
}
