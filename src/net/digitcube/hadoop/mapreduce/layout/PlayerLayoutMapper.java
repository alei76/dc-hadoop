package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑：
 * 
 * 输入：
 * a) 新增玩家 24 个时间片的原始日志(online_first)
 * b) 活跃玩家当天去重日志(依赖 @OnlineDayMapper 的结果)
 * c) 付费玩家当天去重日志(依赖 @PaymentDayMapper 的结果)
 * d) 玩家游戏时长计算结果(依赖 @PayNewUnionActiveMapper 的结果)
 * 
 * 对不同的输入，通过文件名判定为相应的玩家类型，对每个输入，分别在
 * 
 * a) 机型、分辨率、运营商、操作系统、和联网方式
 * b) 地区、性别、帐号类型、年龄、单次游戏时长、游戏时段
 * 
 * 这些维度进行统计
 * 
 * 其中，单次游戏时长 和 游戏时段 的依赖 @PayNewUnionActiveMapper(新增玩家、付费玩家和活跃玩家的关联) 的结果
 * 关联输出结果为：appId, playType, platform, channel, gameServer, loginTime, logoutTime
 * 
 * 输出：
 * key : appId, platId, channel, gameServer, playerType, 维度指标(如联网方式, netType), 维度值(3G)
 * value : 1
 * 
 * 等级升级新增输入及统计
 * 输入：
 * e) 当天滚存的流失玩家信息( @UserInfoRollingDayMapper 的 Constants.SUFFIX_USERFLOW 后缀输出)
 * f) 每台设备帐号数去重后结果(@AccountNumPerDeviceMapper)
 * g) 等级滞停的输出结果(@LevelStopMapper)
 * 
 * 其中
 * 当天滚存的流失玩家信息用于统计各个等级中 7/14/30 流失玩家的分布情况
 * 每台设备帐号数去重后结果用于统计单台设备中帐号数量的分布情况
 * 等级滞停用于统计各个等级 3 天内玩家等级没有升级的玩家数量分布情况
 */

public class PlayerLayoutMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	private final IntWritable levelStat = new IntWritable();
	
	private Calendar cal = Calendar.getInstance();
	
	// startTime,endTime 用于登录时段统计时过滤判断
	// startTime 为当前调度时间推前 1 天的 零时零分零秒  yyyyMM[dd-1] 00:00:00
	// endTime 为当前调度时间的  零时零分零秒 ： yyyyMMdd 00:00:00
	long startTime = 0;
	long endTime = 0;
	private void setStartEndTime(Context context){
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			cal.setTime(date);
		}
		
		//cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		// endTime 当前调度时间的  零时零分零秒 ： yyyyMMdd 00:00:00
		endTime = cal.getTimeInMillis();
		
		// startTime 为当前调度时间推前 1 天的 零时零分零秒  yyyyMM[dd-1] 00:00:00
		cal.add(Calendar.DAY_OF_MONTH, -1);
		startTime = cal.getTimeInMillis();
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//setStartEndTime(context);
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileName.contains(Constants.LOG_FLAG_ONLINE_FIRST)){
			CommonHeader header = new CommonHeader(array);
			CommonExtend extend = new CommonExtend(array);
			
			//设备分
			writeDeviceLayoutResult(  context,
					header.getAppID(),
					header.getPlatform(),
					extend.getChannel(),
					extend.getGameServer(),
					Constants.PLAYER_TYPE_NEWADD,
					
					extend.getBrand(),
					extend.getResolution(),
					extend.getOperators(),
					extend.getOperSystem(),
					extend.getNetType());
			
			//玩家属性分布
			writePlayerLayoutResult(  context,
					header.getAppID(),
					header.getPlatform(),
					extend.getChannel(),
					extend.getGameServer(),
					Constants.PLAYER_TYPE_NEWADD,
					
					extend.getAccountType(),
					extend.getProvince(),
					extend.getGender(),
					extend.getAge());
			
		}else if(fileName.contains(Constants.SUFFIX_ONLINE_DAY)){
			
			OnlineDayLog onlineDayLog = new OnlineDayLog(array);
			
			//设备分布
			writeDeviceLayoutResult(  context,
					onlineDayLog.getAppID(),
					onlineDayLog.getPlatform(),
					onlineDayLog.getExtend().getChannel(),
					onlineDayLog.getExtend().getGameServer(),
					Constants.PLAYER_TYPE_ONLINE,
					
					onlineDayLog.getExtend().getBrand(),
					onlineDayLog.getExtend().getResolution(),
					onlineDayLog.getExtend().getOperators(),
					onlineDayLog.getExtend().getOperSystem(),
					onlineDayLog.getExtend().getNetType());
			
			//玩家属性分布
			writePlayerLayoutResult(  context,
					onlineDayLog.getAppID(),
					onlineDayLog.getPlatform(),
					onlineDayLog.getExtend().getChannel(),
					onlineDayLog.getExtend().getGameServer(),
					Constants.PLAYER_TYPE_ONLINE,

					onlineDayLog.getExtend().getAccountType(),
					onlineDayLog.getExtend().getProvince(),
					onlineDayLog.getExtend().getGender(),
					onlineDayLog.getExtend().getAge());
			
			
			// 等级、升级需求新增：输出玩家人数、游戏次数在等级上的分布（默认为 1 级）
			int level = 0 == onlineDayLog.getMaxLevel() ? 1 : onlineDayLog.getMaxLevel();
			String[] levelPlayerNum = new String[]{ onlineDayLog.getAppID(), 
													onlineDayLog.getPlatform(), 
													onlineDayLog.getExtend().getChannel(), 
													onlineDayLog.getExtend().getGameServer(), 
													Constants.PLAYER_TYPE_ONLINE, 
													Constants.DIMENSION_LEVEL_PLAYER_NUM, //等级人数维度
													""+level};
			mapKeyObj.setOutFields(levelPlayerNum);
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
			context.write(mapKeyObj, one);
			
			//等级游戏次数
			String[] levelLoginTimes = new String[]{ onlineDayLog.getAppID(), 
													onlineDayLog.getPlatform(), 
													onlineDayLog.getExtend().getChannel(), 
													onlineDayLog.getExtend().getGameServer(), 
													Constants.PLAYER_TYPE_ONLINE, 
													Constants.DIMENSION_LEVEL_LOGIN_TIMES, //等级游戏次数维度
													""+level};
			mapKeyObj.setOutFields(levelLoginTimes);
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
			levelStat.set(onlineDayLog.getTotalLoginTimes());
			context.write(mapKeyObj, levelStat);
			
		}else if(fileName.contains(Constants.SUFFIX_PAYMENT_DAY)){
			
			PaymentDayLog paymentDayLog = new PaymentDayLog(array);
			
			//设备分布
			writeDeviceLayoutResult(  context,
					paymentDayLog.getAppID(),
					paymentDayLog.getPlatform(),
					paymentDayLog.getExtend().getChannel(),
					paymentDayLog.getExtend().getGameServer(),
					Constants.PLAYER_TYPE_PAYMENT,
					
					paymentDayLog.getExtend().getBrand(),
					paymentDayLog.getExtend().getResolution(),
					paymentDayLog.getExtend().getOperators(),
					paymentDayLog.getExtend().getOperSystem(),
					paymentDayLog.getExtend().getNetType());
			
			//玩家属性分布
			writePlayerLayoutResult(  context,
					paymentDayLog.getAppID(),
					paymentDayLog.getPlatform(),
					paymentDayLog.getExtend().getChannel(),
					paymentDayLog.getExtend().getGameServer(),
					Constants.PLAYER_TYPE_PAYMENT,

					paymentDayLog.getExtend().getAccountType(),
					paymentDayLog.getExtend().getProvince(),
					paymentDayLog.getExtend().getGender(),
					paymentDayLog.getExtend().getAge());
			
		}else if(fileName.contains(Constants.SUFFIX_PAY_NEW_UNION_ACT)){
			writeTimeLayoutResult(context, array);
			
		}else if(fileName.contains(Constants.SUFFIX_USERFLOW)){
			// 等级、升级需求新增：输出流失玩家在等级上的分布
			String appId = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			String userFlowtype = array[4];
			//String level = array[5];
			// 等级默认用 1 级
			int level = StringUtil.convertInt(array[5], 0);
			level = 0 == level ? 1 : level;
			
			String[] types = getPlayerTypeAndDimenType(userFlowtype);
			if(null == types){
				return;
			}
			
			// 等级、升级需求新增：7/14/30 流失玩家在等级上的分布
			String[] lostPlayerLevel = new String[]{appId, 
												 platform, 
												 channel, 
												 gameServer, 
												 types[0], // 玩家类型
												 types[1], // 统计维度(7/14/30流失玩家停留等级)
												 ""+level};
			mapKeyObj.setOutFields(lostPlayerLevel);
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);//设置后缀为按玩家属性分布
			context.write(mapKeyObj, one);
			
		}else if(fileName.contains(Constants.SUFFIX_ACCOUNT_NUM_PER_DEV)){
			//单台设备帐号数分布统计
			String appid = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			int accountNum = StringUtil.convertInt(array[4],0);
			int accountNumRange = EnumConstants.getAccNumPerDevRange(accountNum);
			
			String[] accNumPerDev = new String[]{appid, platform, channel, gameServer, 
												 Constants.PLAYER_TYPE_NEWADD, 
												 Constants.DIMENSION_DEVICE_ACC_NUM_PER_DEV, 
												 ""+accountNumRange};
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_DEVICE);
			
			mapKeyObj.setOutFields(accNumPerDev);
			context.write(mapKeyObj, one);
		}else if(fileName.contains(Constants.SUFFIX_LEVEL_STOP_STAT)){
			//appid, platform, channel, gameserver, level
			//升级滞停分布统计
			String appid = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			//String level = array[4];
			//等级默认用 1 级
			int level = StringUtil.convertInt(array[4], 0);
			level = 0 == level ? 1 : level;
			
			String[] accNumPerDev = new String[]{appid, platform, channel, gameServer, 
												 Constants.PLAYER_TYPE_ONLINE, 
												 Constants.DIMENSION_LEVEL_STOP, 
												 ""+level};
			mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
			
			mapKeyObj.setOutFields(accNumPerDev);
			context.write(mapKeyObj, one);
		}
	}
	
	private void writeDeviceLayoutResult(Context context,
								   String appId, String platId, String channel, String gameServer, String playerType,
								   String bd, String rl, String op, String os, String nt) 
										   throws IOException, InterruptedException{
		
		//机型
		String[] brand = new String[]{appId, platId, channel, gameServer, playerType, 
									  Constants.DIMENSION_DEVICE_BRAND, bd};
		
		//分辨率
		String[] resolution = new String[]{appId, platId, channel, gameServer, playerType, 
										   Constants.DIMENSION_DEVICE_RESOL, rl}; 
		
		//运营商
		String[] operators = new String[]{appId, platId, channel, gameServer, playerType, 
				   						  Constants.DIMENSION_DEVICE_OPERATOR, op}; 
		
		//操作系统
		String[] opersystem = new String[]{appId, platId, channel, gameServer, playerType, 
					  					   Constants.DIMENSION_DEVICE_OS, os};
		
		//网络介入方式
		String[] nettype = new String[]{appId, platId, channel, gameServer, playerType, 
				   						Constants.DIMENSION_DEVICE_NETTYPE, nt}; 
		
		//设置后缀为按设备分布
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
	}
	
	private void writePlayerLayoutResult(Context context,
			   String appId, String platform, String channel, String gameServer, String playerType,
			   String accoutType, String pronvice, String gender, String age) 
					   throws IOException, InterruptedException{

		/*//渠道
		String[] channelArr = new String[]{appId, platId, gameServer, playerType, 
						  				   Constants.DIMENSION_PLAYER_CHANNEL, channel};*/
		
		//帐号类型
		String[] accoutTypeArr = new String[]{appId, platform, channel, gameServer, playerType, 
							   				  Constants.DIMENSION_PLAYER_ACCOUNTTYPE, accoutType};
				
		//地区
		String[] areaArr = new String[]{appId, platform, channel, gameServer, playerType, 
							   			Constants.DIMENSION_PLAYER_AREA, pronvice}; 
		
		//性别
		String[] genderArr = new String[]{appId, platform, channel, gameServer, playerType, 
							  			  Constants.DIMENSION_PLAYER_GENDER, gender}; 
		
		
		//年龄
		int ageNum = StringUtil.convertInt(age, 0);
		//String ageRange = EnumConstants.Age.getAgeRange(ageNum);
		int ageRange = EnumConstants.getRangeTop4Age(ageNum);
		String[] ageArr = new String[]{appId, platform, channel, gameServer, playerType, 
									   Constants.DIMENSION_PLAYER_AGE, ""+ageRange}; 
		
		/*mapKeyObj.setOutFields(channelArr);
		context.write(mapKeyObj, one);*/
		
		//设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);
				
		mapKeyObj.setOutFields(accoutTypeArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(areaArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(genderArr);
		context.write(mapKeyObj, one);
		
		mapKeyObj.setOutFields(ageArr);
		context.write(mapKeyObj, one);
	}
	
	/*
	 * 登录游戏时段、单次游戏时长分布统计
	 */
	private void writeTimeLayoutResult(Context context, String[] timeInfoArr) throws IOException, InterruptedException{
		
		String appId = timeInfoArr[0];
		String playerType = timeInfoArr[1];
		String platform = timeInfoArr[2];
		String gameServer = timeInfoArr[3];
		String channel = timeInfoArr[4];
		
		/*
		String loginTimeStr = timeInfoArr[5];
		String onlineTimeStr = timeInfoArr[6];
		long loginTime = StringUtil.convertLong(loginTimeStr, 0);
		int onlineTime = StringUtil.convertInt(onlineTimeStr, 0);
		
		if(loginTime <= 0 || onlineTime <=0){
			return;
		}
		*/
		String[] onlineRecords = timeInfoArr[5].split(",");
		
		//设置后缀为按玩家属性分布
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_PLAYER);

		long minLoginTime = 0;
		//取出玩家当天的所有登录信息，并输出单次游戏时长统计
		for(String onlineRecord : onlineRecords){
			String[] record = onlineRecord.split(":");
			if(record.length < 2){
				continue;
			}
			/*long loginTime = StringUtil.convertLong(record[0], 0);
			long onlineTime = StringUtil.convertLong(record[1], 0);*/
			int loginTime = StringUtil.convertInt(record[0], 0);
			int onlineTime = StringUtil.convertInt(record[1], 0);
			//不处理无效的数据
			if(loginTime <= 0 || onlineTime <=0){
				continue;
			}
			
			//单次时长统计结果
			//String singleTimeRange = EnumConstants.GameSingleTime.getSingleTimeRange(onlineTime);
			int singleTimeRange = EnumConstants.getRangeTop4SingleTime(onlineTime);
			String[] singleTime = new String[]{appId, platform, channel, gameServer, playerType,
											   Constants.DIMENSION_PLAYER_SINGLETIME, ""+singleTimeRange};
			mapKeyObj.setOutFields(singleTime);
			context.write(mapKeyObj, one);
			
			//游戏每天首登统计时过滤掉登录时间不在当天的记录
			/*if(loginTime < startTime || onlineTime >= endTime){
				continue;
			}*/
			
			//计算得到一天登录的最小时间
			if(0 == minLoginTime){
				minLoginTime = loginTime;
			}
			minLoginTime = Math.min(loginTime, minLoginTime);
			
			/*//玩家登录游戏时段
			cal.setTimeInMillis(loginTime * 1000); // loginTime 上报时以秒为单位
			loginHourSet.add(cal.get(Calendar.HOUR_OF_DAY));*/
		}
		
		
		// 输出玩家登录游戏时段统计
		// 一个小时里玩家有多次登录的话，只输出一条记录
		/*for(int hour : loginHourSet){
			String[] timePoint = new String[]{appId, platform, channel, gameServer, playerType,
					  						  Constants.DIMENSION_PLAYER_TIMEPOINT, "" + hour};
			mapKeyObj.setOutFields(timePoint);
			context.write(mapKeyObj, one);
		}*/
		
		//玩家当天首次登录游戏的时段
		if(minLoginTime>0){
			cal.setTimeInMillis(minLoginTime * 1000);
			int hour = cal.get(Calendar.HOUR_OF_DAY); //计算玩家首次登录时间(最小登录时间)是在那个时间段
			String[] timePoint = new String[]{appId, platform, channel, gameServer, playerType,
					  Constants.DIMENSION_PLAYER_TIMEPOINT, "" + hour};
			mapKeyObj.setOutFields(timePoint);
			context.write(mapKeyObj, one);
		}

		//时段统计结果
		/*Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(loginTime * 1000); // loginTime 上报时以秒为单位
		loginTime = cal.getTimeInMillis();
		long logoutTime = loginTime + onlineTime * 1000; // onlineTime 也是以秒为单位
		
		String[] timePoint = new String[]{appId, platform, channel, gameServer, playerType,
										  Constants.DIMENSION_PLAYER_TIMEPOINT, ""};
		while(cal.getTimeInMillis() <= logoutTime){
			int hour = cal.get(Calendar.HOUR_OF_DAY);
			timePoint[6] = "" + hour;
			mapKeyObj.setOutFields(timePoint);
			context.write(mapKeyObj, one);
			// calculate hour
			cal.add(Calendar.HOUR_OF_DAY, 1);
		}*/
	}
	
	/**
	 * 根据流失类型解析玩家类型已经流失天数
	 * @param userFlowType
	 * @return
	 */
	private String[] getPlayerTypeAndDimenType(String userFlowType){
		String playerType = "";
		String dimensionType = "";
		
		//新增玩家流失
		if(Constants.UserLostType.NewUserLost7.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		}else if(Constants.UserLostType.NewUserLost14.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		}else if(Constants.UserLostType.NewUserLost30.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_NEWADD;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}
		
		//活跃玩家流失
		else if(Constants.UserLostType.UserLost7.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		}else if(Constants.UserLostType.UserLost14.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		}else if(Constants.UserLostType.UserLost30.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_ONLINE;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}
		
		//付费玩家流失
		else if(Constants.UserLostType.PayUserLost7.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L7_LOST_NUM;
		}else if(Constants.UserLostType.PayUserLost14.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L14_LOST_NUM;
		}else if(Constants.UserLostType.PayUserLost30.value.equals(userFlowType)){
			playerType = Constants.PLAYER_TYPE_PAYMENT;
			dimensionType = Constants.DIMENSION_LEVEL_L30_LOST_NUM;
		}
		
		// 回流或者留存
		else{
			return null;
		}
		
		return new String[]{playerType, dimensionType};
	}
}
