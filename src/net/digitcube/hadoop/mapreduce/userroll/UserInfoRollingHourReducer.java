package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.Constants.UserLostType;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 上午11:07:50 @copyrigt www.digitcube.net
 */

public class UserInfoRollingHourReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	// startTime/endTime 用于判断当天登录时间的有效性
	// 并用这些登录时间计算首充、二充、三充的时间间隔
	private int startTime = 0;
	private int endTime = 0;
	
	//加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	
	//保存同一个帐号对应多个设备的 UID
	//同一个帐号可能在多台设备上进行游戏
	private Set<String> uidSet = new HashSet<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Calendar cal = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			cal.setTime(date);
		}
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		endTime = (int)(cal.getTimeInMillis()/1000);
		
		//开始时间设置为前一天的晚上 8 点
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.add(Calendar.HOUR_OF_DAY, -4);
		startTime = (int)(cal.getTimeInMillis()/1000);
		
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	private Context context;

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		this.context = context;

		uidSet.clear();
		
		// 取结算日期
		int statDate = getStatDate();
		// 本次登陆的在线时长
		int onlineTime = 0;
		// 本次结算的登陆次数
		int totalLoginTimes = 0;
		// 本次结算的付费数量
		int totalCurrencyAmount = 0;
		// 本次登陆之前的最后一次登陆日期，即，若本次有登陆，则是倒数第二次登陆，否则，仍是最后一次登陆
		int lastLoginDate = 0;
		// 本次付费之前的最后一次付费，即，若本次有付费，则是倒数第二次付费，否则，仍是最后一次付费
		int lastPayDate = 0;
		
		// APP 版本号
		// appVerArr[0] = 滚存日志中版本号
		// appVerArr[1] = 注册激活日志中版本号
		// appVerArr[2] = 付费日志中版本号
		// appVerArr[3] = 在线日志中版本号
		// 滚存中取值优先级：在线日志中版本号 > 付费日志中版本号 > 注册日志中版本号 > 滚存日志中版本号
		String newestVersion = null;
		String[] appVerArr = {null,null,null,null};
		
		String[] userRollArray = null;
		String[] regArray = null;
		String[] paymentArray = null;
		String[] onlineArray = null;
		OnlineDayLog onlineDayLog = null;
		
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("U".equals(array[0])) {
				userRollArray = array;
				//设置 APP 版本号
				appVerArr[0] = userRollArray[2];
			} else if ("R".equals(array[0])) {
				regArray = array;
				//新注册的玩家滚存里还不存在，取注册激活里日志里的 APP 版本号
				appVerArr[1] = regArray[regArray.length-1];
				//UID
				uidSet.add(regArray[regArray.length-2]);
			} else if ("P".equals(array[0])) {
				paymentArray = array;
				//付费日志里的 APP 版本号优先于滚存和激活
				appVerArr[2] = paymentArray[3];
			//} else if ("O".equals(array[0])) {
			} else if ("O".equals(value.getSuffix())) {
				onlineArray = array;
				onlineDayLog = new OnlineDayLog(array);
				//在线日志版本号，优先与滚存、激活及滚存的版本号
				appVerArr[3] = onlineDayLog.getAppID().split("\\|")[1];
			}
		}
		
		// 按 在线日志中版本号 > 付费日志中版本号 > 注册日志中版本号 > 滚存日志中版本号 
		// 优先级取 app 版本号
		if(null != appVerArr[3]){
			newestVersion = appVerArr[3]; 
		}else if(null != appVerArr[2]){
			newestVersion = appVerArr[2];
		}else if(null != appVerArr[1]){
			newestVersion = appVerArr[1];
		}else{
			newestVersion = appVerArr[0];
		}
		if(null == newestVersion || "".equals(newestVersion)){
			newestVersion = "1.0";
		}
		
		//UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog();
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(scheduleTime);
		
		String[] keyArray = key.getOutFields();
		// map 端已经把 appId 和 version 拆分开，这里需合并
		// 以兼容依赖于滚存的 MR
		userInfoRollingLog.setAppID(keyArray[0] + "|" +newestVersion);
		userInfoRollingLog.setPlatform(keyArray[1]);
		userInfoRollingLog.setAccountID(keyArray[2]);
		if (userRollArray != null) {
			userInfoRollingLog.setInfoBase64(userRollArray[1]);
		}

		if (regArray != null) {
			// 只取最早的激活和注册时间
			// 激活时间
			if (userInfoRollingLog.getPlayerDayInfo().getActTime() == 0) {
				userInfoRollingLog.getPlayerDayInfo().setActTime(StringUtil.convertInt(regArray[1], 0));
			}
			// 注册时间
			if (userInfoRollingLog.getPlayerDayInfo().getRegTime() == 0) {
				userInfoRollingLog.getPlayerDayInfo().setRegTime(StringUtil.convertInt(regArray[2], 0));
			}
		}
		
		//滚存主要用于记录玩家的在线轨迹
		//如果只有激活注册日志(UserInfo)，只输出激活统计即可
		//不应该输出激活到滚存，因为没有在线状态或流失状态需要记录
		if (userInfoRollingLog.getPlayerDayInfo().getRegTime() == 0 //注册时间为 0
			&& null != regArray && null == onlineArray && null == userRollArray){
			//输出当天激活设备及激活设备中的新增玩家(依赖 UserInfoDay)
			//一台设备中只有一条激活注册记录：
			//如果该设备中有多个新增玩家，则只统计其中一个
			//如果一个玩家有多台激活设备，那么激活设备数累加
			//该需求已经满足统计当天的激活设备数据及激活设备中的新增玩家数量
			
			// 该玩家只是设备激活，而不是激活设备中的新增玩家
			//statNewActDevicePlayer(regArray, userInfoRollingLog, statDate);
			return;
		}
				
		// 在滚存信息被修改前，先获取玩家截至昨天的历史付费次数以及在线时长信息
		// 用于准确计算首充、二充、三充时间间隔
		int historyPayTimes = userInfoRollingLog.getPlayerDayInfo().getTotalPayTimes();
		// 截至昨天的历史总在线时长
		int totalOnlineTime = userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime();
		// 上一次付费的游戏总时长
		int lastPayOnlineTime = userInfoRollingLog.getPlayerDayInfo().getLastPayOnlineTime();
		
		// 倒数第二次付费
		lastPayDate = userInfoRollingLog.getPlayerDayInfo().getLastPayDate();
		if (paymentArray != null) { //
			// 记录渠道
			userInfoRollingLog.getPlayerDayInfo().setChannel(paymentArray[4]);
			// 记录区服
			userInfoRollingLog.getPlayerDayInfo().setGameRegion(paymentArray[5]);
			
			String payGameServer = paymentArray[5];
			String payAppId = keyArray[0];
			if (userInfoRollingLog.getPlayerDayInfo().getFirstPayDate() == 0) {
				// 首付日期为0，则当次是首付费
				userInfoRollingLog.getPlayerDayInfo().setFirstPayDate(statDate);
			}
			totalCurrencyAmount = StringUtil.convertInt(paymentArray[1], 0);
			// 记录当次付费
			userInfoRollingLog.getPlayerDayInfo().setLastPayDate(statDate);
			// 记录总付费
			userInfoRollingLog.getPlayerDayInfo().setTotalCurrencyAmount(
					userInfoRollingLog.getPlayerDayInfo()
							.getTotalCurrencyAmount() + totalCurrencyAmount);
			int payTimes = StringUtil.convertInt(paymentArray[2], 0);
			// 记录付费次数
			userInfoRollingLog.getPlayerDayInfo().setTotalPayTimes(
					userInfoRollingLog.getPlayerDayInfo().getTotalPayTimes()
							+ payTimes);
			// 记录周总付费
			userInfoRollingLog.getPlayerDayInfo().setWeekCurrencyAmount(
					userInfoRollingLog.getPlayerDayInfo()
							.getWeekCurrencyAmount() + totalCurrencyAmount);
			// 记录周总付费次数
			userInfoRollingLog.getPlayerDayInfo().setWeekPayTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getWeekPayTimes() + payTimes));
			// 记录月总付费
			userInfoRollingLog.getPlayerDayInfo().setMonthCurrencyAmount(
					userInfoRollingLog.getPlayerDayInfo()
							.getMonthCurrencyAmount() + totalCurrencyAmount);
			// 记录月总付费次数
			userInfoRollingLog.getPlayerDayInfo().setMonthPayTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getMonthPayTimes() + payTimes));
		}
		// 倒数第二次登陆
		lastLoginDate = userInfoRollingLog.getPlayerDayInfo()
				.getLastLoginDate();
		//if (onlineArray != null) {
		if (onlineDayLog != null) {
			
			if (userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {
				
				//如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				// 首登日期
				userInfoRollingLog.getPlayerDayInfo().setFirstLoginDate(statDate);
				// 如果注册时间没设置 顺便设一下
				if (userInfoRollingLog.getPlayerDayInfo().getRegTime() == 0) {
					userInfoRollingLog.getPlayerDayInfo().setRegTime(statDate);
				}
			}
			//onlineTime = StringUtil.convertInt(onlineArray[2], 0);
			onlineTime = onlineDayLog.getTotalOnlineTime();

			//totalLoginTimes = StringUtil.convertInt(onlineArray[4], 0);
			totalLoginTimes = onlineDayLog.getTotalLoginTimes();
			
			// 记录渠道
			//userInfoRollingLog.getPlayerDayInfo().setChannel(onlineArray[5]);
			userInfoRollingLog.getPlayerDayInfo().setChannel(onlineDayLog.getExtend().getChannel());
			// 记录区服
			//userInfoRollingLog.getPlayerDayInfo().setGameRegion(onlineArray[6]);
			userInfoRollingLog.getPlayerDayInfo().setGameRegion(onlineDayLog.getExtend().getGameServer());
			
			// 记录最后一次登录时间
			userInfoRollingLog.getPlayerDayInfo().setLastLoginDate(statDate);
			// 记录总在线时长
			userInfoRollingLog.getPlayerDayInfo().setTotalOnlineTime(
					userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime()
							+ onlineTime);
			// 记录最后等级
			//userInfoRollingLog.getPlayerDayInfo().setLevel(StringUtil.convertInt(onlineArray[3], 0));
			userInfoRollingLog.getPlayerDayInfo().setLevel(onlineDayLog.getMaxLevel());
			// 记录总登陆次数
			userInfoRollingLog.getPlayerDayInfo().setTotalLoginTimes(
					userInfoRollingLog.getPlayerDayInfo().getTotalLoginTimes()
							+ totalLoginTimes);

			// 记录已玩天数
			userInfoRollingLog.getPlayerDayInfo()
					.setTotalOnlineDay(
							userInfoRollingLog.getPlayerDayInfo()
									.getTotalOnlineDay() + 1);
			// 记录周在线时长
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineTime(
					userInfoRollingLog.getPlayerDayInfo().getWeekOnlineTime()
							+ onlineTime);
			// 记录周登陆次数
			userInfoRollingLog.getPlayerDayInfo().setWeekLoginTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getWeekLoginTimes() + totalLoginTimes));
			// 记录周已玩天数
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineDay(
					(byte) (userInfoRollingLog.getPlayerDayInfo()
							.getWeekOnlineDay() + 1));
			// 记录月在线时长
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineTime(
					userInfoRollingLog.getPlayerDayInfo().getMonthOnlineTime()
							+ onlineTime);
			// 记录月登陆次数
			userInfoRollingLog.getPlayerDayInfo().setMonthLoginTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getMonthLoginTimes() + totalLoginTimes));
			// 记录月已玩天数
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineDay(
					(byte) (userInfoRollingLog.getPlayerDayInfo()
							.getMonthOnlineDay() + 1));

		}

		// 记录用户32天内的登陆情况
		userInfoRollingLog.markLogin(totalLoginTimes > 0);
		// 记录用户32天内的付费情况
		userInfoRollingLog.markPay(totalCurrencyAmount > 0);
		// A. 统计回流用户
		//statUserBack(statDate, lastLoginDate, lastPayDate, userInfoRollingLog);

		// B. 统计流失用户
		//statUserLost(statDate, userInfoRollingLog);

		// C. 统计留存用户
		//statUserStay(statDate, lastLoginDate, lastPayDate, userInfoRollingLog);

		// D. 统计流失漏斗
		//statUserLostFunnel(statDate, totalLoginTimes, userInfoRollingLog);

		// E. 输出玩家习惯在线时间，登陆次数,已玩天数,等级
		statUserHabits(statDate, userInfoRollingLog, onlineTime,
				totalLoginTimes, totalCurrencyAmount);

		// F.输出当日/前7/前30天活跃玩家数，首日/周/月付费数
		statUserPay1730(statDate, userInfoRollingLog);
		
		
		// 统计玩家新增日期、历史充值  add by mikefeng  20141127
		statPlayerNewAddDayAndPay(userInfoRollingLog);
		

		// G. 输出周玩家数据
		//statUserWeek(statDate, userInfoRollingLog);
		// H. 输出月玩家数据
		//statUserMonth(statDate, userInfoRollingLog);
		// I. 记录用户滚存记录
		key.setOutFields(userInfoRollingLog.toStringArray());
		key.setSuffix(Constants.SUFFIX_USERROLLING);
		context.write(key, NullWritable.get());

		// 输出首付信息
		// 首付信息改为在首充、二充、三充时输出，将会得到更准确的结果
		//statUserFirstPay(statDate, userInfoRollingLog);
		
		// J.
		//如果玩家有付费行为,则统计首充、二充、三充的时间间隔
		//并且需要在历史的滚存信息被修改之前统计
		//统计过程中最多只修改滚存信息的上次付费游戏总时长
		//不修改其它任何历史的滚存信息
		if(null != paymentArray && null != onlineDayLog){
			String onlineRecords = onlineDayLog.getOnlineRecords();
			//payTimeInterval(context, statDate, historyPayTimes, totalOnlineTime, lastPayOnlineTime, userInfoRollingLog, paymentArray, onlineRecords);
		}
		
		// K.
		// 到这一步说明激活注册玩家有关联到活跃玩家
		// 否则在开始的时候已经返回
		// 该玩家可能是激活设备中的新增玩家
		if (null != regArray){
			//输出当天激活设备及激活设备中的新增玩家(依赖 UserInfoDay)
			//一台设备中只有一条激活注册记录：
			//如果该设备中有多个新增玩家，则只统计其中一个
			//如果一个玩家有多台激活设备，那么激活设备数累加
			//该需求已经满足统计当天的激活设备数据及激活设备中的新增玩家数量
			//statNewActDevicePlayer(regArray, userInfoRollingLog, statDate);
		}
		
		// L.
		// 输出当前玩家信息，并标记当天是否是新增玩家、当天是否付费
		// 不管当天是否新增或付费，该玩家都是活跃玩家
		boolean isNewPlayer = statDate == userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		boolean isPayToday = null != paymentArray; // 今天是否付费
		if(null != onlineDayLog && null != onlineArray){
			statPlayerOnlineInfo(userInfoRollingLog, onlineDayLog, isNewPlayer, isPayToday);
		}
		//输出新增玩家、新增付费玩家
		//新增付费玩家
		boolean isNewPayPlayer = statDate == userInfoRollingLog.getPlayerDayInfo().getFirstPayDate();
		if(isNewPlayer || isNewPayPlayer){
			statNewAddNewPayPlayer(userInfoRollingLog, isNewPlayer, isNewPayPlayer);
		}
		
		// R.服务器上报付费信息的兼容处理
		// 服务器上报付费信息只保证有这几个字段：appid,accountid,platformtype,paytime,payamout
		// 而其它的渠道、区服、设备等信息则没有
		// 所以需通过滚存关联后用在线信息中的渠道、区服、设备等信息对付费信息进行补全
		if(null != paymentArray){
			statPlayerPayInfo(onlineDayLog, keyArray, paymentArray, isNewPlayer);
		}
	}
	
	
	// 统计玩家新增日期、历史充值  add by mikefeng  20141127
	private void statPlayerNewAddDayAndPay(UserInfoRollingLog userInfoRollingLog)throws IOException,
			InterruptedException {
		OutFieldsBaseModel key = new OutFieldsBaseModel();
		key.setSuffix(Constants.SUFFIX_PLAYER_NEWADD_AND_PAY);
		String accountId = userInfoRollingLog.getAccountID();
		String firstLoginDate = userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() + "";		
		String payTrack = userInfoRollingLog.getPlayerDayInfo().getPayTrack() + "";	
		String totalPayCur = userInfoRollingLog.getPlayerDayInfo().getTotalCurrencyAmount() + "";
		key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,accountId,firstLoginDate,totalPayCur,payTrack));
		context.write(key, NullWritable.get());	
	}

	/**
	 * 统计用户回流数据，打日志
	 * 
	 * @param statDate
	 * @param userInfoRollingLog
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statUserBack(int statDate, int lastLoginDate, int lastPayDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		if (lastLoginDate > 0
				&& userInfoRollingLog.getPlayerDayInfo().getLastLoginDate() == statDate) {
			// 至少是第二次登陆且统计日有登陆

			// 统计新增付费活跃用户的7日回流
			int targetDate = statDate - 7 * 24 * 3600;
			boolean isBackUser = targetDate == lastLoginDate; // 玩家7天前登陆过
			if (isBackUser) {
				boolean isNewUser = targetDate == userInfoRollingLog
						.getPlayerDayInfo().getFirstLoginDate(); //
				boolean isPay = userInfoRollingLog.isPay(targetDate, statDate);
				if (isNewUser) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.NewUserBack7);
				}
				if (isPay) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.PayUserBack7);
				}
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserBack7);
			}
			// 统计新增付费活跃用户的14日回流
			targetDate = statDate - 14 * 24 * 3600;
			isBackUser = targetDate == lastLoginDate; // 玩家14天前登陆过
			if (isBackUser) {
				boolean isNewUser = targetDate == userInfoRollingLog
						.getPlayerDayInfo().getFirstLoginDate(); //
				boolean isPay = userInfoRollingLog.isPay(targetDate, statDate);
				if (isNewUser) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.NewUserBack14);
				}
				if (isPay) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.PayUserBack14);
				}
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserBack14);
			}
			// 统计新增付费活跃用户的30日回流
			targetDate = statDate - 30 * 24 * 3600;
			isBackUser = targetDate == lastLoginDate; // 玩家30天前登陆过
			if (isBackUser) {
				boolean isNewUser = targetDate == userInfoRollingLog
						.getPlayerDayInfo().getFirstLoginDate(); //
				boolean isPay = userInfoRollingLog.isPay(targetDate, statDate);
				if (isNewUser) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.NewUserBack30);
				}
				if (isPay) {
					writeUserFlowLog(userInfoRollingLog,
							UserLostType.PayUserBack30);
				}
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserBack30);
			}
		}
	}

	/**
	 * 统计用户流失数据，打日志
	 * 
	 * @param statDate
	 * @param userInfoRollingLog
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statUserLost(int statDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		int lastLoginDate = userInfoRollingLog.getPlayerDayInfo()
				.getLastLoginDate();
		if (lastLoginDate != 0) {// 计算次日，3日，7日，30日流失

			// 计算次日新增/付费/活跃用户流失
			int targetDate = statDate - 24 * 3600;
			boolean isPay = userInfoRollingLog.isPay(targetDate, statDate);
			boolean isEverLogin = userInfoRollingLog.isEverLogin(1);
			boolean isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			boolean isNewUser = targetDate == userInfoRollingLog
					.getPlayerDayInfo().getFirstLoginDate();
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserLost1);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserLost1);
			}
			if (isLogin && !isEverLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserLost1);
			}
			// 计算3日新增/付费/活跃用户流失
			targetDate = statDate - 3 * 24 * 3600;
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			isEverLogin = userInfoRollingLog.isEverLogin(3);
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserLost3);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserLost3);
			}
			if (isLogin && !isEverLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserLost3);
			}
			// 计算7日新增/付费/活跃用户流失
			targetDate = statDate - 7 * 24 * 3600;
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			isEverLogin = userInfoRollingLog.isEverLogin(7);
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserLost7);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserLost7);
			}
			if (isLogin && !isEverLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserLost7);
			}

			// 计算14日新增/付费/活跃用户流失
			targetDate = statDate - 14 * 24 * 3600;
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			isEverLogin = userInfoRollingLog.isEverLogin(14);
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserLost14);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserLost14);
			}
			if (isLogin && !isEverLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserLost14);
			}

			// 计算30日新增/付费/活跃用户流失
			targetDate = statDate - 30 * 24 * 3600;
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			isEverLogin = userInfoRollingLog.isEverLogin(30);
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserLost30);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserLost30);
			}
			if (isLogin && !isEverLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserLost30);
			}
		}
	}

	private void statUserStay(int statDate, int lastLoginDate, int lastPayDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		if (lastLoginDate != 0
				&& userInfoRollingLog.getPlayerDayInfo().getLastLoginDate() == statDate) { // 昨日有登陆过

			// 计算付费、活跃用户次日留存
			int targetDate = statDate - 24 * 3600;
			boolean isNewUser = targetDate == userInfoRollingLog
					.getPlayerDayInfo().getFirstLoginDate();
			boolean isPay = userInfoRollingLog.isPay(targetDate, statDate);
			boolean isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserStay1);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserStay1);
			}
			if (isLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserStay1);
			}
			// 计算付费活跃用户7日留存
			targetDate = statDate - 7 * 24 * 3600;
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserStay7);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserStay7);
			}
			if (isLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserStay7);
			}
			// 计算付费用户14日留存
			targetDate = statDate - 14 * 24 * 3600;
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserStay14);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserStay14);
			}
			if (isLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserStay14);
			}
			// 计算付费用户30日留存
			targetDate = statDate - 30 * 24 * 3600;
			isNewUser = targetDate == userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate();
			isPay = userInfoRollingLog.isPay(targetDate, statDate);
			isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			if (isNewUser) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.NewUserStay30);
			}
			if (isPay) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.PayUserStay30);
			}
			if (isLogin) {
				writeUserFlowLog(userInfoRollingLog, UserLostType.UserStay30);
			}
		}
	}

	// 统计用户流失漏斗
	private void statUserLostFunnel(int statDate, int totalLoginTimes,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		if (statDate != userInfoRollingLog.getPlayerDayInfo()
				.getLastLoginDate()) {
			// 统计日没有登录，直接返回
			return;
		}
		int i = 10;
		while ((--i) > 0) {
			int targetDate = statDate - i * 24 * 3600;
			boolean isLogin = userInfoRollingLog.isLogin(targetDate, statDate);
			if (!isLogin) {
				// 是否该天活跃用户
				continue;
			}
			// 是否该天新增用户
			boolean isNewUser = userInfoRollingLog.getPlayerDayInfo()
					.getFirstLoginDate() == targetDate;
			// 是否该天付费用户
			boolean isPayUser = userInfoRollingLog.isPay(targetDate, statDate);
			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setSuffix(Constants.SUFFIX_USER_LOST_FUNNEL);
			if (isNewUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
						Constants.PLAYER_TYPE_NEWADD, i, totalLoginTimes));
				context.write(key, NullWritable.get());
			}
			if (isPayUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
						Constants.PLAYER_TYPE_PAYMENT, i, totalLoginTimes));
				context.write(key, NullWritable.get());
			}
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_ONLINE, i, totalLoginTimes));
			context.write(key, NullWritable.get());
		}
	}

	// 统计用户行为习惯
	private void statUserHabits(int statDate,
			UserInfoRollingLog userInfoRollingLog, int onlineTime,
			int totalLoginTimes, int totalCurrencyAmount) throws IOException,
			InterruptedException {
		
		//20141011 Modified by rickpan
		//当玩家当天只有注册日志时，onlineTime 或  totalLoginTimes 可能等于 0 
		//所以只要玩家当天是新增也必须输出统计，忽略 onlineTime 或  totalLoginTimes 是否为 0
		OutFieldsBaseModel key = new OutFieldsBaseModel();
		key.setSuffix(Constants.SUFFIX_USER_HABITS);
		boolean isNewUser = statDate == userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		if (isNewUser) {
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_NEWADD, totalLoginTimes,
					onlineTime, userInfoRollingLog.getPlayerDayInfo()
							.getTotalOnlineDay(), userInfoRollingLog
							.getPlayerDayInfo().getLevel()));
			context.write(key, NullWritable.get());
		}
		
		// 2014-04-28 与 SDK 开发高境、修强确定 onlineTime=0 也视为活跃
		if (onlineTime > 0 || totalLoginTimes > 0) {
			boolean isPayUser = totalCurrencyAmount > 0;
			if (isPayUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
						Constants.PLAYER_TYPE_PAYMENT, totalLoginTimes,
						onlineTime, userInfoRollingLog.getPlayerDayInfo()
								.getTotalOnlineDay(), userInfoRollingLog
								.getPlayerDayInfo().getLevel()));
				context.write(key, NullWritable.get());
			}
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_ONLINE, totalLoginTimes, onlineTime,
					userInfoRollingLog.getPlayerDayInfo().getTotalOnlineDay(),
					userInfoRollingLog.getPlayerDayInfo().getLevel()));
			context.write(key, NullWritable.get());

			// Added at 20140904，增加曾经付过费的玩家的统计
			if (userInfoRollingLog.getPlayerDayInfo().getTotalCurrencyAmount() > 0) {
				key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
						Constants.PLAYER_TYPE_EVER_PAY, totalLoginTimes,
						onlineTime, userInfoRollingLog.getPlayerDayInfo()
								.getTotalOnlineDay(), userInfoRollingLog
								.getPlayerDayInfo().getLevel()));
				context.write(key, NullWritable.get());
			}
		}

	}

	// 输出 -1/-7/-30日活跃玩家数 ,-1/-7/-30 新增用户首日周月付费数
	private void statUserPay1730(int statDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		int lastLoginDate = userInfoRollingLog.getPlayerDayInfo()
				.getLastLoginDate();
		int fistLoginDate = userInfoRollingLog.getPlayerDayInfo()
				.getFirstLoginDate();
		int lastPayDate = userInfoRollingLog.getPlayerDayInfo()
				.getLastPayDate();
		//首付日期
		int fistPayDate = userInfoRollingLog.getPlayerDayInfo().getFirstPayDate();
		OutFieldsBaseModel key = new OutFieldsBaseModel();
		key.setSuffix(Constants.SUFFIX_1730_ACT_PAY_ROLLING);
		if (statDate - lastLoginDate == 0) {
			// -1日活跃
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_ONLINE, Constants.PLAYER_Active_1Day));
			context.write(key, NullWritable.get());
		}
		if (statDate - lastLoginDate < 7 * 24 * 3600) { // 7日活跃
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_ONLINE, Constants.PLAYER_Active_7Day));
			context.write(key, NullWritable.get());
		}
		if (statDate - lastLoginDate < 30 * 24 * 3600) { // 30日活跃
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_ONLINE, Constants.PLAYER_Active_30Day));
			context.write(key, NullWritable.get());
		}
		/*if (statDate - fistLoginDate == 0 && statDate - lastPayDate == 0) { // 新增首日付费
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_1Day));
			context.write(key, NullWritable.get());
		}
		if (statDate - fistLoginDate < 7 * 24 * 3600
				&& statDate - lastPayDate < 7 * 24 * 3600) { // 新增7日内付费
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_7Day));
			context.write(key, NullWritable.get());
		}
		if (statDate - fistLoginDate < 30 * 24 * 3600
				&& statDate - lastPayDate < 30 * 24 * 3600) { // 新增30日内付费
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_30Day));
			context.write(key, NullWritable.get());
		}*/
		// 新增首日付费
		if (statDate - fistLoginDate == 0 && statDate - lastPayDate == 0) {
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_1Day));
			context.write(key, NullWritable.get());
		}
		// 新增7日内付费
		// 如果是  scheduleTime - fistLoginDate, 那么值应该是  7 * 24 * 3600
		// 因为  statDate = scheduleTime - 1day
		// 所以 7 天前新增条件为  statDate - fistLoginDate == (7-1) * 24 * 3600
		if (statDate - fistLoginDate == (7-1) * 24 * 3600 //必须是 7 天前新增的
				&& fistPayDate >= fistLoginDate && fistPayDate - fistLoginDate < 7 * 24 * 3600) { 
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_7Day));
			context.write(key, NullWritable.get());
		}
		// 新增30日内付费
		if (statDate - fistLoginDate == (30-1) * 24 * 3600 //必须是 30 天前新增的
				&& fistPayDate >= fistLoginDate && statDate - lastPayDate < 30 * 24 * 3600) {
			key.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
					Constants.PLAYER_TYPE_PAYMENT, Constants.PLAYER_Pay_30Day));
			context.write(key, NullWritable.get());
		}
	}

	// 输出周滚存数据
	private void statUserWeek(int statDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) statDate * 1000);
		int day_of_week = calendar.get(Calendar.DAY_OF_WEEK);
		if (day_of_week == 1) { // 注意：美国认为周末是一周的开始，所以这里是1！！
								// 计算的日志是周末时，清空该周的数据，并输出周滚存数据,
			int weekLoginTimes = userInfoRollingLog.getPlayerDayInfo()
					.getWeekLoginTimes();

			if (weekLoginTimes == 0)
				return;
			int weekOnlineTime = userInfoRollingLog.getPlayerDayInfo()
					.getWeekOnlineTime();
			byte weekOnlineDay = userInfoRollingLog.getPlayerDayInfo()
					.getWeekOnlineDay();
			// 本周是否首次付费
			boolean isFirstPay = (statDate - userInfoRollingLog
					.getPlayerDayInfo().getFirstPayDate()) < 7 * 24 * 3600;
			// 本周是否首登（新增）
			boolean isFirstLogin = (statDate - userInfoRollingLog
					.getPlayerDayInfo().getFirstLoginDate()) < 7 * 24 * 3600;
			String[] output = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					userInfoRollingLog.getAccountID(),
					isFirstLogin ? "1" : "0",
					isFirstPay ? "1" : "0",
					weekLoginTimes + "",
					weekOnlineTime + "",
					weekOnlineDay + "",
					userInfoRollingLog.getPlayerDayInfo()
							.getWeekCurrencyAmount() + "",
					userInfoRollingLog.getPlayerDayInfo().getWeekPayTimes()
							+ "" };
			OutFieldsBaseModel key = new OutFieldsBaseModel(output);
			key.setSuffix(Constants.SUFFIX_USERROLLING_EVERY_WEEK);
			context.write(key, NullWritable.get());
			// 清零上周数据
			userInfoRollingLog.getPlayerDayInfo().setWeekLoginTimes((short) 0);
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineDay((byte) 0);
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineTime(0);
			userInfoRollingLog.getPlayerDayInfo().setWeekCurrencyAmount(0);
			userInfoRollingLog.getPlayerDayInfo().setWeekPayTimes((short) 0);
		}
	}

	// 输出月滚存数据
	private void statUserMonth(int statDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) statDate * 1000);
		calendar.add(Calendar.DAY_OF_MONTH, 1);// +1天
		int day_of_month = calendar.get(Calendar.DAY_OF_MONTH);
		calendar.setTimeInMillis((long) statDate * 1000);
		if (day_of_month == 1) { // 月初
			int monthLoginTimes = userInfoRollingLog.getPlayerDayInfo()
					.getMonthLoginTimes();
			if (monthLoginTimes == 0)
				return;

			calendar.add(Calendar.DAY_OF_MONTH, -1);
			int year = calendar.get(Calendar.YEAR);
			int month = calendar.get(Calendar.MONTH);
			calendar.setTimeInMillis((long) userInfoRollingLog
					.getPlayerDayInfo().getFirstPayDate() * 1000);
			int year2 = calendar.get(Calendar.YEAR);
			int month2 = calendar.get(Calendar.MONTH);
			calendar.setTimeInMillis((long) userInfoRollingLog
					.getPlayerDayInfo().getFirstLoginDate() * 1000);
			int year3 = calendar.get(Calendar.YEAR);
			int month3 = calendar.get(Calendar.MONTH);
			// 本月是否首次登陆
			boolean isFirstLogin = (year == year3 && month3 == month);
			// 本月是否首次付费
			boolean isFirstPay = (year == year2 && month2 == month);

			int monthOnlineTime = userInfoRollingLog.getPlayerDayInfo()
					.getMonthOnlineTime();
			byte monthOnlineDay = userInfoRollingLog.getPlayerDayInfo()
					.getMonthOnlineDay();

			String[] output = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					userInfoRollingLog.getAccountID(),
					isFirstLogin ? "1" : "0",
					isFirstPay ? "1" : "0",
					monthLoginTimes + "",
					monthOnlineTime + "",
					monthOnlineDay + "",
					userInfoRollingLog.getPlayerDayInfo()
							.getMonthCurrencyAmount() + "",
					userInfoRollingLog.getPlayerDayInfo().getMonthPayTimes()
							+ "" };
			OutFieldsBaseModel key = new OutFieldsBaseModel(output);
			key.setSuffix(Constants.SUFFIX_USERROLLING_EVERY_MONTH);
			context.write(key, NullWritable.get());
			// 清零上月数据
			userInfoRollingLog.getPlayerDayInfo().setMonthLoginTimes((short) 0);
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineDay((byte) 0);
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineTime(0);
			userInfoRollingLog.getPlayerDayInfo().setMonthCurrencyAmount(0);
			userInfoRollingLog.getPlayerDayInfo().setMonthPayTimes((short) 0);
		}
	}

	// 打印用户流动相关日志，流失/回流/留存
	private void writeUserFlowLog(UserInfoRollingLog userInfoRollingLog,
			Constants.UserLostType type) throws IOException,
			InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(Constants.SUFFIX_USERFLOW);
		userLostKey.setOutFields(getStardKeyWithArgs(userInfoRollingLog,
				type.value, userInfoRollingLog.getPlayerDayInfo().getLevel()));
		context.write(userLostKey, NullWritable.get());
	}

	private int getStatDate() {
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		//该类用于当天小时任务，统计时间不用减 1
		//calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}

	/**
	 * 取某天的起始时间
	 * 
	 * @return
	 */
	private int getStartDate(int time) {
		long timestamp = (long) time * 1000;
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}

	private String[] getStardKeyWithArgs(UserInfoRollingLog userInfoRollingLog,
			Object... args) {
		List<String> list = new ArrayList<String>();
		list.add(userInfoRollingLog.getAppID());
		list.add(userInfoRollingLog.getPlatform());
		list.add(userInfoRollingLog.getPlayerDayInfo().getChannel());
		list.add(userInfoRollingLog.getPlayerDayInfo().getGameRegion());
		if (args != null) {
			for (Object arg : args) {
				list.add(arg + "");
			}
		}
		return list.toArray(new String[0]);
	}

	/*// 输出首付信息
	 * 首付信息改为在首充、二充、三充时输出，将会得到更准确的结果
	private void statUserFirstPay(int statDate,
			UserInfoRollingLog userInfoRollingLog) throws IOException,
			InterruptedException {

		// Check if any data is invalid
		if (StringUtil.isEmpty(userInfoRollingLog.getAppID())
				|| StringUtil.isEmpty(userInfoRollingLog.getPlatform())
				|| StringUtil.isEmpty(userInfoRollingLog.getPlayerDayInfo()
						.getChannel())
				|| StringUtil.isEmpty(userInfoRollingLog.getPlayerDayInfo()
						.getGameRegion())) {

			// Invalid, just return
			return;
		}

		// 如果当前结算时间等于玩家的首付时间，则输出玩家首付统计信息
		if (statDate == userInfoRollingLog.getPlayerDayInfo().getFirstPayDate()) {

			// 首付时的在线天数、时长、级别、首付金额
			// 首付游戏天数需求更改：首付日期 - 首登日期
			// int totalOnlineDay = userInfoRollingLog.getPlayerDayInfo().getTotalOnlineDay();
			int firstPayDate = userInfoRollingLog.getPlayerDayInfo().getFirstPayDate();
			int firstLoginDate = userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
			int totalOnlineDay = (firstPayDate - firstLoginDate)/3600/24;
									
			int totalOnlineTime = userInfoRollingLog.getPlayerDayInfo()
					.getTotalOnlineTime();
			int level = userInfoRollingLog.getPlayerDayInfo().getLevel();
			// 首付时，首付金额 = totalCurrencyAmount
			int totalCurrencyAmount = userInfoRollingLog.getPlayerDayInfo()
					.getTotalCurrencyAmount();
			// TODO : 礼包(PaymentDay 日志中未输入该值，PaymentDay 需要改造以提供该值)

			// 获取首付时的在线天数、时长、级别、首付金额范围，用于维度统计
			int onlineDayRange = EnumConstants.getGameDaysRange(totalOnlineDay);
			int onlineTimeRange = EnumConstants
					.getGameTimeRange(totalOnlineTime);
			int levelRange = EnumConstants.getFirstPayLevelRange(level);
			int currencyRange = EnumConstants
					.getFirstPayCurRange(totalCurrencyAmount);

			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setSuffix(Constants.SUFFIX_FIRST_PAY_DAY);

			// 输出结果
			// 首付时玩家游戏天数
			String[] gameDaysArr = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					Constants.DIMENSION_PLAYER_DAYS, "" + onlineDayRange };
			key.setOutFields(gameDaysArr);
			context.write(key, NullWritable.get());

			// 首付时玩家游戏时长
			String[] gameTimeArr = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					Constants.DIMENSION_PLAYER_ONLINETIME, "" + onlineTimeRange };
			key.setOutFields(gameTimeArr);
			context.write(key, NullWritable.get());

			// 首付时玩家游戏等级
			String[] gameLevelArr = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					Constants.DIMENSION_PLAYER_LEVEL, "" + levelRange };
			key.setOutFields(gameLevelArr);
			context.write(key, NullWritable.get());

			// 首付金额 currency
			String[] gameCurrencyArr = new String[] {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
					Constants.DIMENSION_FIRST_PAY_GAMECURR, "" + currencyRange };
			key.setOutFields(gameCurrencyArr);
			context.write(key, NullWritable.get());
		}
	}*/

	/**
	 * 
	 * @param context
	 * @param statDate
	 * @param historyPayTimes	历史总付费次数
	 * @param historyOnlineTime	截至昨天为止，玩家总在线时长
	 * @param lastPayOnlineTime	玩家上次付费时总在线时长
	 * @param userHistoryInfo
	 * @param paymentArray
	 * @param onlineArray
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void payTimeInterval(Context context, int statDate, 
					int historyPayTimes, int historyOnlineTime, int lastPayOnlineTime,
					UserInfoRollingLog userHistoryInfo, String[] paymentArray, String onlineRecords) throws IOException, InterruptedException{
		//如果玩家的付费次数已经大于等于 3 次，则不再进行付费时隔统计
		if(historyPayTimes >=3 || null == userHistoryInfo || null == paymentArray || null == onlineRecords || "".equals(onlineRecords)){
			return;
		}
		
		// onlineRecord 里记录了该玩家当天所有登录时间以及对应的在线时长
		//String[] onlineRecords = onlineArray[onlineArray.length - 1].split(",");
		String[] records = onlineRecords.split(",");
		TreeMap<Integer, Integer> onlineMap = new TreeMap<Integer, Integer>();
		for(String record : records){
			String[] arr = record.split(":");
			if(arr.length > 1){
				int loginTime = StringUtil.convertInt(arr[0], 0);
				int onlineTime = StringUtil.convertInt(arr[1], 0);
				// 登录时间不合法，则过滤
				if(loginTime <= 0 || onlineTime <= 0 || loginTime < startTime || loginTime > endTime){
					continue;
				}
				
				onlineMap.put(loginTime, onlineTime);
			}
		}
		/*if(0 == onlineMap.size()){
			return;
		}*/
		
		// paymentRecord 里记录了该玩家当天前三次的付费时间以及对应的等级
		// (目前业务需求值统计前三次充值时间间隔)
		String[] paymentRecords = paymentArray[paymentArray.length - 1].split(",");
		TreeMap<Integer, String> paymentMap = new TreeMap<Integer, String>();
		for(String record : paymentRecords){
			String[] arr = record.split(":");
			int payTime = StringUtil.convertInt(arr[0], 0);
			if(payTime <= 0){
				continue;
			}
			
			paymentMap.put(payTime, record);
		}
		if(0 == paymentMap.size()){
			return;
		}
		
		// 分别计算每次付费距离当天首登的时长
		Set<Integer> payTimeSet = paymentMap.keySet();
		int[] payTimeArr = new int[payTimeSet.size() > 3 ? 3 : payTimeSet.size()];
		int i=0;
		for(int payTime : payTimeSet){
			if(i >= payTimeArr.length){
				break;
			}
			
			// 每次付费在当天里的在线时长(时长间隔 = payTimeArr[i] - payTimeArr[i-1])
			payTimeArr[i] = calculatePayTime(onlineMap,payTime);
			i++;
		}
		
		OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
		keyObj.setSuffix(Constants.SUFFIX_PAY_TIME_INTERVAL);
		
		// 计算首充、二充、三充时间间隔
		//int historyOnlineTime = userHistoryInfo.getPlayerDayInfo().getTotalOnlineTime();
		//int lastPayOnlineTime = userHistoryInfo.getPlayerDayInfo().getLastPayOnlineTime();
		int firstPayTimeInterval = 0;
		int secondPayTimeInterval = 0;
		int thirdPayTimeInterval = 0;
		
		if(0 == historyPayTimes){
			if(payTimeArr.length > 2){
				firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
				secondPayTimeInterval = payTimeArr[1] - payTimeArr[0];
				thirdPayTimeInterval = payTimeArr[2] - payTimeArr[1];
				userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[2]);
				
				// first pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_1, firstPayTimeInterval);
				// second pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_2, secondPayTimeInterval);
				// third pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_3, thirdPayTimeInterval);
				
				/*// 输出首付信息
				String paymentRecord = paymentMap.firstEntry().getValue();
				statUserFirstPay(statDate, userHistoryInfo, paymentRecord, firstPayTimeInterval);*/
			}else if(payTimeArr.length > 1){
				firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
				secondPayTimeInterval = payTimeArr[1] - payTimeArr[0];
				userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[1]);
				
				// first pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_1, firstPayTimeInterval);
				// second pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_2, secondPayTimeInterval);
				
				/*// 输出首付信息
				String paymentRecord = paymentMap.firstEntry().getValue();
				statUserFirstPay(statDate, userHistoryInfo, paymentRecord, firstPayTimeInterval);*/
			}else if(payTimeArr.length == 1){
				firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
				userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[0]);
				
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_1, firstPayTimeInterval);
				
				/*// 输出首付信息
				String paymentRecord = paymentMap.firstEntry().getValue();
				statUserFirstPay(statDate, userHistoryInfo, paymentRecord, firstPayTimeInterval);*/
			}
			
			// 输出首付信息
			if(firstPayTimeInterval>0){
				String paymentRecord = paymentMap.firstEntry().getValue();
				statUserFirstPay(statDate, userHistoryInfo, paymentRecord, firstPayTimeInterval);
			}
		}else if(1 == historyPayTimes){
			if(payTimeArr.length > 1){
				secondPayTimeInterval = historyOnlineTime - lastPayOnlineTime + payTimeArr[0];
				thirdPayTimeInterval = payTimeArr[1] - payTimeArr[0];
				userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[1]);
				
				// second pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_2, secondPayTimeInterval);
				// third pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_3, thirdPayTimeInterval);
			}else if(payTimeArr.length == 1){
				secondPayTimeInterval = historyOnlineTime - lastPayOnlineTime + payTimeArr[0];
				userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[0]);
				// second pay time interval
				writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_2, secondPayTimeInterval);
			}
			
		}else if(2 == historyPayTimes){
			if(payTimeArr.length > 0){
				thirdPayTimeInterval = historyOnlineTime - lastPayOnlineTime + payTimeArr[0];
				 userHistoryInfo.getPlayerDayInfo().setLastPayOnlineTime(historyOnlineTime + payTimeArr[0]);
				 // third pay time interval
				 writePayTimeInterval(context, keyObj, userHistoryInfo, Constants.DIMENSION_PAY_TIME_INTERVAL_3, thirdPayTimeInterval);
			}
		}
	}
	
	private void writePayTimeInterval(Context ctx, OutFieldsBaseModel keyObj, UserInfoRollingLog userHistoryInfo, 
			String payTimeIntervalType, int payTimeInvertal) throws IOException, InterruptedException{
		
		int payTimeRange = EnumConstants.getPayTimeRange(payTimeInvertal);
		keyObj.setOutFields(new String[]{
				userHistoryInfo.getAppID(),
				userHistoryInfo.getPlatform(),
				userHistoryInfo.getPlayerDayInfo().getChannel(),
				userHistoryInfo.getPlayerDayInfo().getGameRegion(),
				userHistoryInfo.getAccountID(),
				//Constants.DIMENSION_PAY_TIME_INTERVAL,
				payTimeIntervalType,
				""+payTimeRange
		});
		context.write(keyObj, NullWritable.get());
	}
	private int calculatePayTime(TreeMap<Integer, Integer> onlineMap, int payTime){
		Integer floorLoginTime = onlineMap.floorKey(payTime);
		if(null == floorLoginTime){
			return 0;
		}
		
		int todayOnlineTime = 0;
		Set<Integer> loginTimeSet = onlineMap.keySet();
		for(Integer loginTime : loginTimeSet){
			if(loginTime >= floorLoginTime){
				break;
			}
			todayOnlineTime += onlineMap.get(loginTime);
		}
		
		todayOnlineTime = todayOnlineTime + (payTime - floorLoginTime); 
		return todayOnlineTime;
	}
	
	// 输出首付信息
	private void statUserFirstPay(int statDate,
			UserInfoRollingLog userInfoRollingLog,
			String paymentRecord, int firstPayOnlineTime) throws IOException,
			InterruptedException {

		// paymentRecord 应该为 payTime:payCurrency:payLevel
		String[] payArr = paymentRecord.split(":");
		if(payArr.length <= 2){
			return;
		}
		
		// 首付时的在线天数 = 首付日期(当天) - 首登日期
		int firstLoginDate = userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		// 0 == firstLoginDate 表明是首登当天就付费了，此时记首付在线天数为 1
		int totalOnlineDay = (0 == firstLoginDate) ? 1 : (statDate - firstLoginDate)/3600/24;
		// 首付时的在线时长
		int totalOnlineTime = firstPayOnlineTime;
		// 首付金额
		int firstPayCurrency = StringUtil.convertInt(payArr[1],0);
		// 首付等级
		int firstPayLevel = StringUtil.convertInt(payArr[2],0);
		// TODO : 礼包(PaymentDay 日志中未输入该值，PaymentDay 需要改造以提供该值)

		// 获取首付时的在线天数、时长、级别、首付金额范围，用于维度统计
		int onlineDayRange = EnumConstants.getFirstPayGameDaysRange(totalOnlineDay);
		//int onlineTimeRange = EnumConstants.getGameTimeRange(totalOnlineTime);
		int onlineTimeRange = EnumConstants.getPayTimeRange(totalOnlineTime);
		int levelRange = EnumConstants.getFirstPayLevelRange(firstPayLevel);
		int currencyRange = EnumConstants.getFirstPayCurRange(firstPayCurrency);

		OutFieldsBaseModel key = new OutFieldsBaseModel();
		key.setSuffix(Constants.SUFFIX_FIRST_PAY_DAY);

		// 输出结果
		// 首付时玩家游戏天数
		String[] gameDaysArr = new String[] {
				userInfoRollingLog.getAppID(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
				Constants.DIMENSION_PLAYER_DAYS, "" + onlineDayRange };
		key.setOutFields(gameDaysArr);
		context.write(key, NullWritable.get());

		// 首付时玩家游戏时长
		String[] gameTimeArr = new String[] {
				userInfoRollingLog.getAppID(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
				Constants.DIMENSION_PLAYER_ONLINETIME, "" + onlineTimeRange };
		key.setOutFields(gameTimeArr);
		context.write(key, NullWritable.get());

		// 首付时玩家游戏等级
		String[] gameLevelArr = new String[] {
				userInfoRollingLog.getAppID(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
				Constants.DIMENSION_PLAYER_LEVEL, "" + levelRange };
		key.setOutFields(gameLevelArr);
		context.write(key, NullWritable.get());

		// 首付金额 currency
		String[] gameCurrencyArr = new String[] {
				userInfoRollingLog.getAppID(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
				Constants.DIMENSION_FIRST_PAY_GAMECURR, "" + currencyRange };
		key.setOutFields(gameCurrencyArr);
		context.write(key, NullWritable.get());
	}
	
	//输出激活新增信息
	private void statNewActDevicePlayer(String[] regArray, UserInfoRollingLog userInfoRollingLog, int statDate) throws IOException, InterruptedException{
		
		boolean isNewPlayer = statDate == userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		int actTime = StringUtil.convertInt(regArray[1], 0);
		if(actTime > 0){
			String uids = "";
			for(String uid : uidSet){
				if(uidSet.size() <= 50){
					//同一账户下 UID 数小于等于 50 时，记录全部真实 UID
					uids = uids + "," + uid;
				}else{
					//同一账户下 UID 数大于 50 时，只记录简单符号
					uids = uids + "," + "U";
				}
			}
			uids = uids.replaceFirst(",", "");
			
			//只统计激活设备及激活设备中的新增玩家
			//所以只输出激活时间大于 0 的记录
			String[] newActDevicePlayer = {
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getPlatform(),
					regArray[3], //channel
					regArray[4], //gameServer
					userInfoRollingLog.getAccountID(), //accountId
					regArray[5], //accountNum
					uids, // uid
					isNewPlayer ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO
					
			};
			
			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setOutFields(newActDevicePlayer);
			key.setSuffix(Constants.SUFFIX_NEW_ACTDEVICE_PLAYER);
			context.write(key, NullWritable.get());
		}
	}
	
	//输出玩家在线信息，并标识是否新增、付费
	private void statPlayerOnlineInfo(UserInfoRollingLog userInfoRollingLog, OnlineDayLog onlineDayLog, 
			boolean isNewPlayer, boolean isPayToday) throws IOException, InterruptedException{
		String playerType = Constants.DATA_FLAG_PLAYER_ONLINE; //只是活跃
		if(isNewPlayer && isPayToday){ //集新增、活跃、付费于一身
			playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY; 
		}else if(isNewPlayer){ //只是新增、活跃
			playerType = Constants.DATA_FLAG_PLAYER_NEW_ONLINE; 
		}else if(isPayToday){ //只是活跃、付费
			playerType = Constants.DATA_FLAG_PLAYER_ONLINE_PAY; 
		}
		
		// Added at 20140805 
		// 为支持玩家两次登录时间间隔统计而增加的修改
		// a) 用今天的最后登录时间更新滚存中的最后登录时间
		// b) 把历史上最后一次的登录时间保存到今天的最后登录时间中
		int lastLoginTime = userInfoRollingLog.getPlayerDayInfo().getLastLoginTime();
		userInfoRollingLog.getPlayerDayInfo().setLastLoginTime(onlineDayLog.getLastLoginTime());
		onlineDayLog.setLastLoginTime(lastLoginTime);
		
		String[] onlineArray = onlineDayLog.toStringArray();
		int length = onlineArray.length + 1;
		String[] onlineInfoArr = new String[length];
		System.arraycopy(onlineArray, 0, onlineInfoArr, 0, onlineArray.length);
		onlineInfoArr[length - 1] = playerType;
				
		OutFieldsBaseModel outKey = new OutFieldsBaseModel();
		outKey.setSuffix(Constants.SUFFIX_PLAYER_ONLINE_INFO);
		outKey.setOutFields(onlineInfoArr);
		context.write(outKey, NullWritable.get());
	}
	
	// 输出当天的新增玩家、新增付费玩家
	private void statNewAddNewPayPlayer(UserInfoRollingLog userInfoRollingLog, boolean isNewPlayer, boolean isNewPayPlayer) throws IOException, InterruptedException{
		String[] keyFields = new String[]{
				userInfoRollingLog.getAppID(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getGameRegion(),
				userInfoRollingLog.getAccountID(),
				isNewPlayer ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
				isNewPayPlayer ? Constants.DATA_FLAG_YES : Constants.DATA_FLAG_NO,
		};
		OutFieldsBaseModel outKey = new OutFieldsBaseModel();
		outKey.setSuffix(Constants.SUFFIX_NEWADD_NEWPAY_PLAYER);
		outKey.setOutFields(keyFields);
		context.write(outKey, NullWritable.get());
	}
	
	// 揉合用户在线数据到用户付费数据中
	// 20180827 : 付费统计中分出新增玩家和活跃玩家，所以增加 isNewPlayer 参数
	private void statPlayerPayInfo(OnlineDayLog onlineDayLog, String[] keyFields, String[] paymentArray, boolean isNewPlayer) throws IOException, InterruptedException{
		if(null == paymentArray || 0 == paymentArray.length){
			return;
		}
		PaymentDayLog payment = new PaymentDayLog();
		OutFieldsBaseModel outKey = new OutFieldsBaseModel();
		outKey.setSuffix(Constants.SUFFIX_PLAYER_PAY_INFO);
		
		if(null != onlineDayLog){
			payment.setAppID(onlineDayLog.getAppID());
			payment.setPlatform(onlineDayLog.getPlatform());
			payment.setAccountID(onlineDayLog.getAccountID());
			payment.setExtend(onlineDayLog.getExtend());
			//int totalCurrencyAmount = StringUtil.convertInt(paymentArray[1], 0);
			float totalCurrencyAmount = StringUtil.convertFloat(paymentArray[1], 0);
			int totalPayTimes = StringUtil.convertInt(paymentArray[2], 0);
			String payRecords = paymentArray[paymentArray.length - 1];
			payment.setCurrencyAmount(totalCurrencyAmount);
			payment.setTotalPayTimes(totalPayTimes);
			payment.setPayRecords(payRecords);
			
		}else{
			
			int i = 0;
			String appId = keyFields[i++];
			String platform = keyFields[i++];
			String accountId = keyFields[i++];
			String gameServer = keyFields[i++];
			
			i = 1;
			//int totalCurrencyAmount = StringUtil.convertInt(paymentArray[i++], 0);
			float totalCurrencyAmount = StringUtil.convertFloat(paymentArray[i++], 0);
			int totalPayTimes = StringUtil.convertInt(paymentArray[i++], 0);
			String appVersion = paymentArray[i++];
			String channel = paymentArray[i++];
			String gameSrv = paymentArray[i++];
			String payRecords = paymentArray[i++];
			
			payment.setAppID(appId + "|" + appVersion);
			payment.setPlatform(platform);
			payment.setAccountID(accountId);
			
			//付费日志没有在线关联，所以无法知道设备、渠道、区服等
			CommonExtend extend = new CommonExtend(new String[]{
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
					"unknow",
			}, -5);
			extend.setGameServer(gameServer);
			extend.setChannel(channel);
			payment.setExtend(extend);
			
			payment.setCurrencyAmount(totalCurrencyAmount);
			payment.setTotalPayTimes(totalPayTimes);
			payment.setPayRecords(payRecords);
		}
		
		// 20180827 : 付费统计中分出新增玩家和活跃玩家
		// 滚存统计之后  PaymentDayLog 中没有多余字段标识这是当天新增玩家
		// 经查之后，下游依赖 MR 中没有使用 PaymentDayLog 中 resolution 字段
		// 所以这里把新增玩家标识设置到 resolution 字段中
		// 下游 MR 根据该字段判断是否新增玩家
		if(isNewPlayer){
			payment.getExtend().setResolution(Constants.PLAYER_TYPE_NEWADD);
		}else{
			payment.getExtend().setResolution(Constants.PLAYER_TYPE_ONLINE);
		}
		
		outKey.setOutFields(payment.toStringArray());
		context.write(outKey, NullWritable.get());
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
