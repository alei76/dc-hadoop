package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.SuffixMultipleOutputFormat;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 
 * 主要逻辑：
 * 
 * 输入： a) 滚存中输出的玩家在线信息(@UserInfoRollingDayMapper -->
 * Constants.SUFFIX_PLAYER_ONLINE_INFO) 该输入包括新增活跃付费玩家的信息，分别在： 1)
 * 设备（机型、分辨率、运营商、操作系统、和联网方式） 2) 玩家属性（地区、性别、帐号类型、年龄）进行分布统计 3)
 * 同时又对这些玩家的单次游戏时长和游戏时段进行统计
 * 
 * b) 当天滚存的流失玩家信息(@UserInfoRollingDayMapper --> Constants.SUFFIX_USERFLOW) c)
 * 每台设备帐号数去重后结果(@AccountNumPerDeviceMapper) d) 等级滞停的输出结果(@LevelStopMapper)
 * 
 * 其中 当天滚存的流失玩家信息用于统计各个等级中 7/14/30 流失玩家的分布情况 每台设备帐号数去重后结果用于统计单台设备中帐号数量的分布情况
 * 等级滞停用于统计各个等级 3 天内玩家等级没有升级的玩家数量分布情况
 * 
 * 
 * Map： key : appId, platId, channel, gameServer, playerType, 维度指标(如联网方式,
 * netType), 维度值(3G) val : 1
 * 
 * Reduce : appId, platId, channel, gameServer, playerType, 维度指标(如联网方式,
 * netType), 维度值(3G), sum(1)
 * 
 */

public class PlayerInfoLayoutMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private static final Log LOGGER = LogFactory
			.getLog(PlayerInfoLayoutMapper.class);

	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private String fileName = "";

	private final IntWritable levelStat = new IntWritable();
	private Calendar cal = Calendar.getInstance();

	// 添加是否小时任务 add by mikefeng 20141013
	private boolean isHour = false;
	private int currentHour = 23;

	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Date scheduleTime = ConfigManager.getInitialDate(
				context.getConfiguration(), new Date());
		currentHour = scheduleTime.getHours();
		isHour = context.getConfiguration().getBoolean("is.hour.job", false);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);

		if (fileName.contains(Constants.SUFFIX_PLAYER_ONLINE_INFO)) {
			String playerType = array[array.length - 1];
			String[] onlineDayArr = new String[array.length - 1];
			System.arraycopy(array, 0, onlineDayArr, 0, array.length - 1);
			OnlineDayLog onlineDayLog = new OnlineDayLog(onlineDayArr);

			String[] playerTypeArr = null;
			if (Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY.equals(playerType)) {
				playerTypeArr = new String[] { Constants.PLAYER_TYPE_NEWADD, // 新增
						Constants.PLAYER_TYPE_ONLINE, // 活跃
						Constants.PLAYER_TYPE_PAYMENT // 付费
				};
			} else if (Constants.DATA_FLAG_PLAYER_NEW_ONLINE.equals(playerType)) {
				playerTypeArr = new String[] { Constants.PLAYER_TYPE_NEWADD, // 新增
						Constants.PLAYER_TYPE_ONLINE // 活跃
				};
			} else if (Constants.DATA_FLAG_PLAYER_ONLINE_PAY.equals(playerType)) {
				playerTypeArr = new String[] { Constants.PLAYER_TYPE_ONLINE, // 活跃
						Constants.PLAYER_TYPE_PAYMENT // 付费
				};
			} else if (Constants.DATA_FLAG_PLAYER_ONLINE.equals(playerType)) {
				playerTypeArr = new String[] { Constants.PLAYER_TYPE_ONLINE // 活跃
				};
			} else {
				return;
			}

			// aggregate online time range stat
			for (OutFieldsBaseModel out_field : calculateUserOnlineTimeDiestribution(
					onlineDayLog, playerTypeArr)) {
				context.write(out_field, one);
			}

			for (String playerTp : playerTypeArr) {
				// 1. 输出新增活跃付费玩家在设备及玩家属性上的分布统计
				// 设备分布统计
				writeDeviceLayoutResult(context, playerTp, onlineDayLog);

				// 玩家属性分布
				writePlayerLayoutResult(context, onlineDayLog.getAppID(),
						onlineDayLog.getPlatform(), onlineDayLog.getExtend()
								.getChannel(), onlineDayLog.getExtend()
								.getGameServer(), playerTp,

						onlineDayLog.getExtend().getAccountType(), onlineDayLog
								.getExtend().getCountry(), onlineDayLog
								.getExtend().getProvince(), onlineDayLog
								.getExtend().getGender(), onlineDayLog
								.getExtend().getAge());

				// 2. 输出新增活跃付费玩家上的分布统计
				// a)首次登录所属时段分布
				// b)单次游戏时长次数分布
				// c)单次游戏时长人数分布
				writeTimeLayoutResult(context, onlineDayLog, playerTp);

				// 新增玩家首次游戏时长
				if (Constants.PLAYER_TYPE_NEWADD.equals(playerTp)) {
					String[] onlineRecords = onlineDayLog.getOnlineRecords()
							.split(",");
					if (onlineRecords.length < 1) {
						return;
					}
					String[] record = onlineRecords[0].split(":");
					if (record.length < 2) {
						return;
					}

					int onlineTime = StringUtil.convertInt(record[1], 0);
					int singleTimeRange = EnumConstants
							.getRangeTop4SingleTime(onlineTime);
					String[] singleTime = new String[] {
							onlineDayLog.getAppID(),
							onlineDayLog.getPlatform(),
							onlineDayLog.getExtend().getChannel(),
							onlineDayLog.getExtend().getGameServer(),
							Constants.PLAYER_TYPE_NEWADD,
							Constants.DIMENSION_FIRST_GAME_TIME,
							"" + singleTimeRange };
					mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
					mapKeyObj.setOutFields(singleTime);
					context.write(mapKeyObj, one);
				}

				// a) 统计玩家每次登录所属时段
				// b) 统计玩家两次登录的时间间隔
				writeLoginTimeInfo(context, onlineDayLog, playerTp);
			}

			// 3. 输出活跃玩家在等级上的人数及游戏次数统计（等级默认为 1 级）
			// 不管玩家当天是否新增或付费，它都是活跃玩家
			writeLevelLayoutResult(context, onlineDayLog);

		} else if (fileName.contains(Constants.SUFFIX_USERFLOW)) {
			// 等级、升级需求新增：输出流失玩家在等级上的分布
			String appId = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			String userFlowtype = array[4];
			// String level = array[5];
			// 等级默认用 1 级
			int level = StringUtil.convertInt(array[5], 0);
			level = 0 == level ? 1 : level;

			String[] types = getPlayerTypeAndDimenType(userFlowtype);
			if (null == types) {
				return;
			}

			// 等级、升级需求新增：7/14/30 流失玩家在等级上的分布
			String[] lostPlayerLevel = new String[] { appId, platform, channel,
					gameServer, types[0], // 玩家类型
					types[1], // 统计维度(7/14/30流失玩家停留等级)
					"" + level };
			mapKeyObj.setOutFields(lostPlayerLevel);
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);// 设置后缀为按玩家属性分布
			context.write(mapKeyObj, one);

		}
		/*
		 * 单设备帐号数已在统计激活设备新增时一并计算 else
		 * if(fileName.endsWith(Constants.SUFFIX_ACT)){ UserInfoLog userInfoLog
		 * = new UserInfoLog(array); if(userInfoLog.getRegTime() > 0){
		 * //单台设备帐号数分布统计 String appid = userInfoLog.getAppID(); String platform
		 * = userInfoLog.getPlatform(); String channel =
		 * userInfoLog.getChannel(); String gameServer =
		 * userInfoLog.getGameServer(); int accountNum = 0 ==
		 * userInfoLog.getAccountNum() ? 1 : userInfoLog.getAccountNum(); int
		 * accountNumRange = EnumConstants.getAccNumPerDevRange(accountNum);
		 * 
		 * String[] accNumPerDev = new String[]{appid, platform, channel,
		 * gameServer, Constants.PLAYER_TYPE_NEWADD,
		 * Constants.DIMENSION_DEVICE_ACC_NUM_PER_DEV, ""+accountNumRange};
		 * mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_DEVICE);
		 * 
		 * mapKeyObj.setOutFields(accNumPerDev); context.write(mapKeyObj, one);
		 * } }
		 */
		else if (fileName.contains(Constants.SUFFIX_LEVEL_STOP_STAT)) {
			// appid, platform, channel, gameserver, level
			// 升级滞停分布统计
			String appid = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			// String level = array[4];
			// 等级默认用 1 级
			int level = StringUtil.convertInt(array[4], 0);
			level = 0 == level ? 1 : level;

			String[] accNumPerDev = new String[] { appid, platform, channel,
					gameServer, Constants.PLAYER_TYPE_ONLINE,
					Constants.DIMENSION_LEVEL_STOP, "" + level };
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);

			mapKeyObj.setOutFields(accNumPerDev);
			context.write(mapKeyObj, one);
		}
	}

	protected List<OutFieldsBaseModel> calculateUserOnlineTimeDiestribution(
			OnlineDayLog log, String[] player_type_attrs) {
		// maintain user distribution
		Set<Integer> distributions = new TreeSet<Integer>();

		// reape times
		Set<Integer> times = new TreeSet<Integer>();
		for (String record : log.getOnlineRecords().split(",")) {
			String[] pack = record.split(":");

			// sanity check
			if (pack.length != 2) {
				LOGGER.warn("record sanity check fail,package expected to be length 2, but got:"
						+ pack.length + " raw:" + log + " ignoring.");
				continue;
			}

			times.add(StringUtil.convertInt(pack[0], 0));
		}

		int last = -1;
		for (Integer time : times) {
			if (time == 0) {
				LOGGER.warn("unexpetected zero time,for raw log:" + log
						+ " online longreacords:" + log.getOnlineRecords());
				continue;
			}

			// mark first
			if (last == -1) {
				last = time;
				continue;
			}

			// bookeepping
			distributions
					.add(EnumConstants.getRangeTop4SingleTime(time - last));

			// update last
			last = time;
		}

		// build result
		List<OutFieldsBaseModel> result = new ArrayList<OutFieldsBaseModel>(
				distributions.size());
		for (int range : distributions) {
			// attach context
			for (String play_type_attr : player_type_attrs) {
				OutFieldsBaseModel out = new OutFieldsBaseModel();

				// filter set
				out.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);

				// for each attributes
				out.setOutFields(new String[] {
						log.getAppID(), // appid id
						log.getPlatform(), // platform
						log.getExtend().getChannel(), // channel
						log.getExtend().getGameServer(), // game server
						play_type_attr, // player type attras
						Constants.DIMENSION_LOGINTIME_PER_RANGE_USERS,
						"" + range // range belongings
				});

				// add to result
				result.add(out);
			}
		}
		return result;
	}

	private void writeDeviceLayoutResult(Context context, String playerType,
			OnlineDayLog onlineDayLog) throws IOException, InterruptedException {

		String appId = onlineDayLog.getAppID();
		String platId = onlineDayLog.getPlatform();
		String channel = onlineDayLog.getExtend().getChannel();
		String gameServer = onlineDayLog.getExtend().getGameServer();
		String bd = onlineDayLog.getExtend().getBrand();
		String rl = onlineDayLog.getExtend().getResolution();
		String op = onlineDayLog.getExtend().getOperators();
		String os = onlineDayLog.getExtend().getOperSystem();
		String nt = onlineDayLog.getExtend().getNetType();
		String simCardCode = onlineDayLog.getSimCradOp();// 20141025 SIM 卡运营商

		// 机型
		String[] brand = new String[] { appId, platId, channel, gameServer,
				playerType, Constants.DIMENSION_DEVICE_BRAND, bd };

		// 分辨率
		String[] resolution = new String[] { appId, platId, channel,
				gameServer, playerType, Constants.DIMENSION_DEVICE_RESOL, rl };

		// 运营商
		String[] operators = new String[] { appId, platId, channel, gameServer,
				playerType, Constants.DIMENSION_DEVICE_OPERATOR, op };

		// 操作系统
		String[] opersystem = new String[] { appId, platId, channel,
				gameServer, playerType, Constants.DIMENSION_DEVICE_OS, os };

		// 网络介入方式
		String[] nettype = new String[] { appId, platId, channel, gameServer,
				playerType, Constants.DIMENSION_DEVICE_NETTYPE, nt };

		// 设置后缀为按设备分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_DEVICE);

		mapKeyObj.setOutFields(brand);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(resolution);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(operators);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(opersystem);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(nettype);
		context.write(mapKeyObj, one);
		// SIM 卡运营商
		String[] simCodes = simCardCode.split(",");
		for (String simCode : simCodes) {
			if (simCode.trim().isEmpty()) {
				continue;
			}
			String[] simCardOper = new String[] { appId, platId, channel,
					gameServer, playerType,
					Constants.DIMENSION_SIM_CARD_OPERATOR, simCode };
			mapKeyObj.setOutFields(simCardOper);
			context.write(mapKeyObj, one);
		}
	}

	private void writePlayerLayoutResult(Context context, String appId,
			String platform, String channel, String gameServer,
			String playerType, String accoutType, String country,
			String pronvice, String gender, String age) throws IOException,
			InterruptedException {

		/*
		 * //渠道 String[] channelArr = new String[]{appId, platId, gameServer,
		 * playerType, Constants.DIMENSION_PLAYER_CHANNEL, channel};
		 */

		// 帐号类型
		String[] accoutTypeArr = new String[] { appId, platform, channel,
				gameServer, playerType, Constants.DIMENSION_PLAYER_ACCOUNTTYPE,
				accoutType };

		// 地区
		String[] areaArr = new String[] { appId, platform, channel, gameServer,
				playerType, Constants.DIMENSION_PLAYER_AREA, pronvice };
		// cntry
		String[] cntryArr = new String[] { appId, platform, channel,
				gameServer, playerType, Constants.DIMENSION_PLAYER_COUNTRY,
				country };

		// 性别
		String[] genderArr = new String[] { appId, platform, channel,
				gameServer, playerType, Constants.DIMENSION_PLAYER_GENDER,
				gender };

		// 年龄
		int ageNum = StringUtil.convertInt(age, 0);
		// String ageRange = EnumConstants.Age.getAgeRange(ageNum);
		int ageRange = EnumConstants.getRangeTop4Age(ageNum);
		String[] ageArr = new String[] { appId, platform, channel, gameServer,
				playerType, Constants.DIMENSION_PLAYER_AGE, "" + ageRange };

		// 设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);

		mapKeyObj.setOutFields(accoutTypeArr);
		context.write(mapKeyObj, one);

		// 省份
		mapKeyObj.setOutFields(areaArr);
		context.write(mapKeyObj, one);
		// 国家
		mapKeyObj.setOutFields(cntryArr);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(genderArr);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(ageArr);
		context.write(mapKeyObj, one);
	}

	/*
	 * 登录游戏时段、单次游戏时长分布统计
	 */
	private void writeTimeLayoutResult(Context context,
			OnlineDayLog onlineDayLog, String playerType) throws IOException,
			InterruptedException {

		String appId = onlineDayLog.getAppID();
		String platform = onlineDayLog.getPlatform();
		String gameServer = onlineDayLog.getExtend().getGameServer();
		String channel = onlineDayLog.getExtend().getChannel();

		String[] onlineRecords = onlineDayLog.getOnlineRecords().split(",");

		// 设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);

		// 单次游戏时长人数统计
		// 玩家有多次游戏时长相同只算一次
		Set<Integer> singleTimeSet = new HashSet<Integer>();

		long minLoginTime = 0;
		// 取出玩家当天的所有登录信息，并输出单次游戏时长统计
		for (String onlineRecord : onlineRecords) {
			String[] record = onlineRecord.split(":");
			if (record.length < 2) {
				continue;
			}

			int loginTime = StringUtil.convertInt(record[0], 0);
			int onlineTime = StringUtil.convertInt(record[1], 0);
			/*
			 * //20140729 : 高境约定修改 时长小于等于 0 则取 1
			 * 
			 * //不处理无效的数据 if(loginTime <= 0 || onlineTime <=0){ continue; }
			 */
			onlineTime = onlineTime <= 0 ? 1 : onlineTime;

			// 单次时长统计结果
			int singleTimeRange = EnumConstants
					.getRangeTop4SingleTime(onlineTime);
			String[] singleTime = new String[] { appId, platform, channel,
					gameServer, playerType,
					Constants.DIMENSION_PLAYER_SINGLETIME, "" + singleTimeRange };
			mapKeyObj.setOutFields(singleTime);
			context.write(mapKeyObj, one);

			// 单次游戏时长人数统计
			// 玩家有多次游戏时长相同只算一次
			singleTimeSet.add(singleTimeRange);

			// 计算得到一天登录的最小时间
			if (0 == minLoginTime) {
				minLoginTime = loginTime;
			}
			minLoginTime = Math.min(loginTime, minLoginTime);

		}

		// 玩家当天首次登录游戏的时段
		if (minLoginTime > 0) {
			cal.setTimeInMillis(minLoginTime * 1000);
			int hour = cal.get(Calendar.HOUR_OF_DAY); // 计算玩家首次登录时间(最小登录时间)是在那个时间段
			String[] timePoint = new String[] { appId, platform, channel,
					gameServer, playerType,
					Constants.DIMENSION_PLAYER_TIMEPOINT, "" + hour };
			mapKeyObj.setOutFields(timePoint);
			context.write(mapKeyObj, one);
		}

		// 单次游戏时长人数统计
		for (int singleTimeRange : singleTimeSet) {
			String[] singleTime = new String[] { appId, platform, channel,
					gameServer, playerType,
					Constants.DIMENSION_PLAYER_SINGLETIME_PlayerNum,
					singleTimeRange + "" };
			mapKeyObj.setOutFields(singleTime);
			context.write(mapKeyObj, one);
		}
	}

	/*
	 * 输出玩家在等级上人数及游戏次数的统计
	 */
	private void writeLevelLayoutResult(Context context,
			OnlineDayLog onlineDayLog) throws IOException, InterruptedException {
		int level = 0 == onlineDayLog.getMaxLevel() ? 1 : onlineDayLog
				.getMaxLevel();
		String[] levelPlayerNum = new String[] { onlineDayLog.getAppID(),
				onlineDayLog.getPlatform(),
				onlineDayLog.getExtend().getChannel(),
				onlineDayLog.getExtend().getGameServer(),
				Constants.PLAYER_TYPE_ONLINE,
				Constants.DIMENSION_LEVEL_PLAYER_NUM, // 等级人数维度
				"" + level };
		mapKeyObj.setOutFields(levelPlayerNum);
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
		context.write(mapKeyObj, one);

		// 等级游戏次数
		String[] levelLoginTimes = new String[] { onlineDayLog.getAppID(),
				onlineDayLog.getPlatform(),
				onlineDayLog.getExtend().getChannel(),
				onlineDayLog.getExtend().getGameServer(),
				Constants.PLAYER_TYPE_ONLINE,
				Constants.DIMENSION_LEVEL_LOGIN_TIMES, // 等级游戏次数维度
				"" + level };
		mapKeyObj.setOutFields(levelLoginTimes);
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
		levelStat.set(onlineDayLog.getTotalLoginTimes());
		context.write(mapKeyObj, levelStat);
	}

	/*
	 * 根据流失类型解析玩家类型已经流失天数
	 */
	private String[] getPlayerTypeAndDimenType(String userFlowType) {
		String playerType = "";
		String dimensionType = "";

		// 新增玩家流失
		if (Constants.UserLostType.NewUserLost7.value.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		} else if (Constants.UserLostType.NewUserLost14.value
				.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		} else if (Constants.UserLostType.NewUserLost30.value
				.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}

		// 活跃玩家流失
		else if (Constants.UserLostType.UserLost7.value.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		} else if (Constants.UserLostType.UserLost14.value.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		} else if (Constants.UserLostType.UserLost30.value.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}

		// 付费玩家流失
		else if (Constants.UserLostType.PayUserLost7.value.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		} else if (Constants.UserLostType.PayUserLost14.value
				.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		} else if (Constants.UserLostType.PayUserLost30.value
				.equals(userFlowType)) {
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}

		// 回流或者留存
		else {
			return null;
		}

		return new String[] { playerType, dimensionType };
	}

	/**
	 * a) 统计玩家每次登录所属时段 b) 统计玩家两次登录的时间间隔
	 * 
	 * @param context
	 * @param onlineDayLog
	 * @param playerType
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeLoginTimeInfo(Context context, OnlineDayLog onlineDayLog,
			String playerType) throws IOException, InterruptedException {

		String[] onlineRecords = onlineDayLog.getOnlineRecords().split(",");

		// 设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
		// 登录时长间隔
		TreeSet<Integer> loginTimeSet = new TreeSet<Integer>();
		// Added at 20140731
		// 注意：onlineDayLog.getLastLoginTime 字段
		// a) 在 OnlineDay MR 输出时，该字段保存的是当天的最后登录时间
		// b) 在滚出输出时，该字段保存的是出当天外历史中最后一次最后登录时间
		// 所以这里为了统计两次登录时间间隔时，必须把历史中最后一次最后登录时间加上
		loginTimeSet.add(onlineDayLog.getLastLoginTime());

		// 取出玩家当天的所有登录信息，并输出单次游戏时长统计
		for (String onlineRecord : onlineRecords) {
			String[] record = onlineRecord.split(":");
			if (record.length < 2) {
				continue;
			}

			int loginTime = StringUtil.convertInt(record[0], 0);
			// 不处理无效的数据
			if (loginTime <= 0) {
				continue;
			}

			// 记录登录时间，用户统计登录间隔
			loginTimeSet.add(loginTime);

			cal.setTimeInMillis(loginTime * 1000L);
			int hour = cal.get(Calendar.HOUR_OF_DAY);

			// 过滤掉 hour 当天大于调度时间的记录
			if (isHour && hour > currentHour) {
				continue;
			}

			// 玩家登录游戏时段
			String[] timePoint = new String[] { onlineDayLog.getAppID(),
					onlineDayLog.getPlatform(),
					onlineDayLog.getExtend().getChannel(),
					onlineDayLog.getExtend().getGameServer(), playerType,
					Constants.DIMENSION_TIMEPOINT_NUM, hour + "" };
			mapKeyObj.setOutFields(timePoint);
			context.write(mapKeyObj, one);
		}

		// 登录间隔时长分布
		int lastLoginTime = 0;
		for (Integer loginTime : loginTimeSet) {

			// 如果是小时任务则需要 loginTime的小时数<当前任务实例时间的小时数 add by mikefeng 20141013
			/*
			 * if(isHour){ if((DateUtil.getHourByMsecond(loginTime*1000L) >
			 * DateUtil.getHourByMsecond(scheduleTime.getTime())) || loginTime
			 * <= 0 ){ continue; } }
			 */

			if (0 == lastLoginTime) {
				lastLoginTime = loginTime;
			} else {
				int loginTimeInterval = loginTime - lastLoginTime;
				loginTimeInterval = EnumConstants
						.getRangeTop4LoginTime(loginTimeInterval);
				String[] loginInterval = new String[] {
						onlineDayLog.getAppID(), onlineDayLog.getPlatform(),
						onlineDayLog.getExtend().getChannel(),
						onlineDayLog.getExtend().getGameServer(), playerType,
						Constants.DIMENSION_LOGINTIME_INTERTVAL,
						loginTimeInterval + "" };
				mapKeyObj.setOutFields(loginInterval);
				context.write(mapKeyObj, one);

				//
				lastLoginTime = loginTime;
			}
		}
	}

	public static void main(String[] args) {
		UserGroupInformation.createRemoteUser("hadoop").doAs(
				new PrivilegedAction<Void>() {
					@Override
					public Void run() {
						try {
							Configuration configuration = new Configuration();

							// base input path
							Path input_base = new Path(
									"/data/digitcube/rolling_day/"
											+ new SimpleDateFormat("yyyy/MM/dd")
													.format(new Date())
											+ "/00/output");

							// find inputs
							FileSystem fs = FileSystem.get(configuration);
							List<Path> inputs = new ArrayList<Path>();
							for (FileStatus status : fs.listStatus(input_base,
									new PathFilter() {
										@Override
										public boolean accept(Path path) {
											return path.getName().endsWith(
													"PLAYER_ONLINE_INFO");
										}
									})) {
								inputs.add(status.getPath());
							}

							// setup output
							Path output = new Path("/tmp/test_player_info");
							if (fs.exists(output)) {
								fs.delete(output, true);
							}

							// enable compress
							JobConf conf = new JobConf(configuration);
							conf.setMapOutputCompressorClass(SnappyCodec.class);

							// setup job
							Job job = new Job(conf);

							// job basic config
							job.setJobName("test_player_info");
							job.setJarByClass(PlayerInfoLayoutMapper.class);

							// setup input
							job.setInputFormatClass(TextInputFormat.class);
							FileInputFormat.setInputPaths(job,
									inputs.toArray(new Path[0]));

							// setput ouput
							job.setOutputFormatClass(SuffixMultipleOutputFormat.class);
							FileOutputFormat.setOutputPath(job, output);

							// setup mapper
							job.setMapperClass(PlayerInfoLayoutMapper.class);
							job.setMapOutputKeyClass(OutFieldsBaseModel.class);
							job.setMapOutputValueClass(IntWritable.class);

							// setup reducer
							job.setReducerClass(PlayerInfoLayoutReducer.class);
							job.setOutputKeyClass(OutFieldsBaseModel.class);
							job.setOutputValueClass(IntWritable.class);

							// setup combiner
							// job.setCombinerClass(PlayerInfoLayoutReducer.class);

							// set num of reducer
							job.setNumReduceTasks(10);

							// disable speculate
							job.setMapSpeculativeExecution(false);
							job.setReduceSpeculativeExecution(false);

							// submit
							job.waitForCompletion(true);
						} catch (Exception e) {
							LOGGER.error("fail to submit job", e);
						}
						return null;
					}
				});
	}
}
