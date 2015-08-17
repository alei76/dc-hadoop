package net.digitcube.hadoop.mapreduce.userroll.week;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants.UserLostType;
import net.digitcube.hadoop.mapreduce.domain.UserInfoWeekRolling;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7周31日 下午8:50:11 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserInfoRollingWeekReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private Context context;

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	private int weekDate_1 = 0;
	private int weekDate_2 = 0;
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		this.context = context;
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(),new Date());
		statDate = getStatDate(); // 因为默认是结算程序运行的前一天，这里日期应该是每周末

		//20150120 用于判断流失/回流
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(1000L * statDate);
		calendar.add(Calendar.DAY_OF_MONTH, -7); //减 7 天为上周最后一天
		weekDate_1 = (int)(calendar.getTimeInMillis() / 1000);
		
		calendar.add(Calendar.DAY_OF_MONTH, -7); //减 7 天为上周最后一天
		weekDate_2 = (int)(calendar.getTimeInMillis() / 1000);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] userRollArray = null; // 历史滚存
		String[] userWeekInfoArray = null; // 本周用户详情

		//int statDate = getStatDate(); // 因为默认是结算程序运行的前一天，这里日期应该是每周末

		// APP 版本号
		// appVerArr[0] = 历史周滚存中版本号
		// appVerArr[1] = 日滚存日志中版本号
		// 滚存中取值优先级：日滚存日志中版本号 > 历史周滚存中版本号
		String newestVersion = null;
		String[] appVerArr = {null, null};
		
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("A".equals(array[0])) {
				userWeekInfoArray = array;
				
				//日滚存版本号
				appVerArr[1] = array[array.length - 1];
				
			} else if ("B".equals(array[0])) {
				userRollArray = array;
				
				//历史周滚存版本号
				appVerArr[0] = array[array.length - 1];
			}
		}
		// 按 日滚存日志中版本号 > 历史周滚存中版本号 
		// 优先级取 app 版本号
		if(null != appVerArr[1]){
			newestVersion = appVerArr[1]; 
		}else if(null != appVerArr[0]){
			newestVersion = appVerArr[0];
		}
		if(null == newestVersion || "".equals(newestVersion)){
			newestVersion = "1.0";
		}
		
		// 取key中的appid，platform，accountid
		String[] keyArray = key.getOutFields();
		// UserInfoWeekRolling userInfoWeekRolling = new UserInfoWeekRolling();
		UserInfoWeekRolling userInfoWeekRolling = new UserInfoWeekRolling(
				scheduleTime);
		//userInfoWeekRolling.setAppID(keyArray[0]);
		userInfoWeekRolling.setAppID(keyArray[0] + "|" + newestVersion); // 合并 appId 和 Version
		userInfoWeekRolling.setPlatform(keyArray[1]);
		userInfoWeekRolling.setAccountID(keyArray[2]);
		if (userRollArray != null) {
			userInfoWeekRolling.setInfoBase64(userRollArray[1]);
		}
		userInfoWeekRolling.getPlayerWeekInfo();
		// 处理上周滚存的信息
		int loginTimes = 0;
		int onlineTime = 0;
		int onlineDay = 0;
		int currencyAmount = 0;
		int payTimes = 0;
		if (userWeekInfoArray != null) {
			String channel = userWeekInfoArray[1];
			String gameRegion = userWeekInfoArray[2];
			boolean isFirstLogin = "1".equals(userWeekInfoArray[3]);
			boolean isFirstPay = "1".equals(userWeekInfoArray[4]);
			loginTimes = StringUtil.convertInt(userWeekInfoArray[5], 0);
			onlineTime = StringUtil.convertInt(userWeekInfoArray[6], 0);
			onlineDay = StringUtil.convertInt(userWeekInfoArray[7], 0);
			currencyAmount = StringUtil.convertInt(userWeekInfoArray[8], 0);
			payTimes = StringUtil.convertInt(userWeekInfoArray[9], 0);
			// 更新渠道信息
			userInfoWeekRolling.getPlayerWeekInfo().setChannel(channel);
			userInfoWeekRolling.getPlayerWeekInfo().setGameRegion(gameRegion);
			// 更新登陆信息
			if (userInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate() == 0) {
				if (isFirstLogin) {
					userInfoWeekRolling.getPlayerWeekInfo()
							.setFirstLoginWeekDate(statDate);
					userInfoWeekRolling.getPlayerWeekInfo()
							.setLastLoginWeekDate(statDate);
				}
			} else {
				userInfoWeekRolling.getPlayerWeekInfo().setLastLoginWeekDate(
						statDate);
			}
			// 更新付费信息
			if (userInfoWeekRolling.getPlayerWeekInfo().getFirstPayWeekDate() == 0) {
				if (isFirstPay) {
					userInfoWeekRolling.getPlayerWeekInfo()
							.setFirstPayWeekDate(statDate);
					userInfoWeekRolling.getPlayerWeekInfo().setLastPayWeekDate(
							statDate);
				}
			} else {
				userInfoWeekRolling.getPlayerWeekInfo().setLastPayWeekDate(
						statDate);
			}
		}
		// 标记登陆
		userInfoWeekRolling.markLogin(loginTimes > 0);
		// 标记付费
		userInfoWeekRolling.markPay(currencyAmount > 0);
		// A. 统计流失漏斗
		statUserLostFunnel(statDate, loginTimes, userInfoWeekRolling);
		// B. 流失
		statUserLost(statDate, userInfoWeekRolling);
		// C. 回流
		statUserBack(statDate, userInfoWeekRolling);
		
		// 输出滚存数据
		key.setOutFields(userInfoWeekRolling.toStringArray());
		key.setSuffix(Constants.SUFFIX_USERROLLING_WEEK);
		context.write(key, NullWritable.get());
	}

	// 统计用户流失漏斗
	private void statUserLostFunnel(int statDate, int loginTimes,
			UserInfoWeekRolling userInfoWeekRolling) throws IOException,
			InterruptedException {
		if (loginTimes == 0) {
			// 统计日没有登录，直接返回
			return;
		}
		int i = 10;
		while ((--i) > 0) {
			// i周之前
			int targetDate = statDate - i * 7 * 24 * 3600;
			boolean isLogin = userInfoWeekRolling.isLogin(targetDate, statDate);
			if (!isLogin) // 是否该天活跃用户
				continue;
			// 是否该天新增用户
			boolean isNewUser = userInfoWeekRolling.getPlayerWeekInfo()
					.getFirstLoginWeekDate() == targetDate;
			// 是否该天付费用户
			boolean isPayUser = userInfoWeekRolling.isPay(targetDate, statDate);
			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setSuffix(Constants.SUFFIX_USER_LOST_FUNNEL_WEEK);
			if (isNewUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoWeekRolling,
						Constants.PLAYER_TYPE_NEWADD, i, loginTimes));
				context.write(key, NullWritable.get());
			}
			if (isPayUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoWeekRolling,
						Constants.PLAYER_TYPE_PAYMENT, i, loginTimes));
				context.write(key, NullWritable.get());
			}
			key.setOutFields(getStardKeyWithArgs(userInfoWeekRolling,
					Constants.PLAYER_TYPE_ONLINE, i, loginTimes));
			context.write(key, NullWritable.get());

		}
	}

	private String[] getStardKeyWithArgs(
			UserInfoWeekRolling userInfoWeekRolling, Object... args) {
		List<String> list = new ArrayList<String>();
		list.add(userInfoWeekRolling.getAppID());
		list.add(userInfoWeekRolling.getPlatform());
		list.add(userInfoWeekRolling.getPlayerWeekInfo().getChannel());
		list.add(userInfoWeekRolling.getPlayerWeekInfo().getGameRegion());
		if (args != null) {
			for (Object arg : args) {
				list.add(arg + "");
			}
		}
		return list.toArray(new String[0]);
	}

	private int getStatDate() {
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}

	/**
	 * oldLastLoginDate : 上次的最后登录日期
	 * 
	 * @param statDate
	 * @param oldLastLoginDate
	 * @param oldLastPayDate
	 * @param UserInfoWeekRolling
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statUserBack(int statDate, UserInfoWeekRolling UserInfoWeekRolling) 
			throws IOException, InterruptedException {
		//本周必须有登录才计算回流
		if (statDate == UserInfoWeekRolling.getPlayerWeekInfo().getLastLoginWeekDate()) {
			// 上上周是否登录
			boolean isLoginWeekDate_2 = UserInfoWeekRolling.isLogin(weekDate_2, statDate);
			// 上周是否登录
			boolean isLoginWeekhDate_1 = UserInfoWeekRolling.isLogin(weekDate_1, statDate);
			// 本周是否登录
			boolean isLoginWeekDate_0 = UserInfoWeekRolling.isLogin(statDate, statDate);
			
			// 1 周回流条件：上上周有登录，上周无登录，本周有登录
			boolean isBackUser = isLoginWeekDate_2 && (!isLoginWeekhDate_1) && isLoginWeekDate_0;
			if (isBackUser) {
				boolean isNewUser = weekDate_2 == UserInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate();
				boolean isPay = UserInfoWeekRolling.isPay(weekDate_2, statDate);
				boolean isEverPayBeforeBackDay = UserInfoWeekRolling.getPlayerWeekInfo().getFirstPayWeekDate() > 0;
				
				writeUserFlowLog(weekDate_2, UserInfoWeekRolling, Constants.PLAYER_TYPE_ONLINE, Constants.SUFFIX_USER_BACK_WEEK_1);
				if (isNewUser) {
					writeUserFlowLog(weekDate_2, UserInfoWeekRolling, Constants.PLAYER_TYPE_NEWADD, Constants.SUFFIX_USER_BACK_WEEK_1);
				}
				if (isPay) {
					writeUserFlowLog(weekDate_2, UserInfoWeekRolling, Constants.PLAYER_TYPE_PAYMENT, Constants.SUFFIX_USER_BACK_WEEK_1);
				}
				if (isEverPayBeforeBackDay) {
					writeUserFlowLog(weekDate_2, UserInfoWeekRolling, Constants.PLAYER_TYPE_EVER_PAY, Constants.SUFFIX_USER_BACK_WEEK_1);
				}
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
	private void statUserLost(int statDate, UserInfoWeekRolling UserInfoWeekRolling) throws IOException,
			InterruptedException {
		
		//本周必须无登录才计算流失
		if (statDate != UserInfoWeekRolling.getPlayerWeekInfo().getLastLoginWeekDate()) {
			//上周是否登录
			boolean isLoginWeekDate_1 = UserInfoWeekRolling.isLogin(weekDate_1, statDate);
			//本周是否登录
			boolean isLoginWeekDate_0 = UserInfoWeekRolling.isLogin(statDate, statDate);
			
			// 1 周流失条件：上周有登录，本周无登录
			if (isLoginWeekDate_1 && (!isLoginWeekDate_0)) {
				//是否上周新增
				boolean isNewUser = weekDate_1 == UserInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate();
				//上周是否付费
				boolean isPay = UserInfoWeekRolling.isPay(weekDate_1, statDate);
				//是否曾经付费
				boolean isEverPay = UserInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate() > 0;
				
				writeUserFlowLog(weekDate_1, UserInfoWeekRolling, Constants.PLAYER_TYPE_ONLINE, Constants.SUFFIX_USER_LOST_WEEK_1);
				if (isNewUser) {
					writeUserFlowLog(weekDate_1, UserInfoWeekRolling, Constants.PLAYER_TYPE_NEWADD, Constants.SUFFIX_USER_LOST_WEEK_1);
				}
				if (isPay) {
					writeUserFlowLog(weekDate_1, UserInfoWeekRolling, Constants.PLAYER_TYPE_PAYMENT, Constants.SUFFIX_USER_LOST_WEEK_1);
				}
				if (isEverPay) {
					writeUserFlowLog(weekDate_1, UserInfoWeekRolling, Constants.PLAYER_TYPE_EVER_PAY, Constants.SUFFIX_USER_LOST_WEEK_1);
				}
			}
		}
	}
	
	/**
	 * 把统计时间直接输出到结果中，入库是直接入前台表，不必再转换
	 * 如 1 周流失的 statTime 为上周，1 周回流的 statTime 为上上周
	 * @param statTime
	 * @param UserInfoWeekRolling
	 * @param type
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeUserFlowLog(int statTime, UserInfoWeekRolling UserInfoWeekRolling, 
			String playerType, String suffix) 
			throws IOException, InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(suffix);
		String[] keyFields = new String[]{
				statTime+"",
				UserInfoWeekRolling.getAppID(),
				UserInfoWeekRolling.getPlatform(),
				UserInfoWeekRolling.getPlayerWeekInfo().getChannel(),
				UserInfoWeekRolling.getPlayerWeekInfo().getGameRegion(),
				UserInfoWeekRolling.getAccountID(),
				playerType
		};
		userLostKey.setOutFields(keyFields);
		context.write(userLostKey, NullWritable.get());
	}
	
	// 打印用户流动相关日志，流失/回流/留存
	private void writeUserFlowLog(UserInfoWeekRolling UserInfoWeekRolling,
			Constants.UserLostType type) throws IOException,
			InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(Constants.SUFFIX_USERFLOW);
		userLostKey.setOutFields(getStardKeyWithArgs(UserInfoWeekRolling,
				type.value, UserInfoWeekRolling.getAccountID()));
		context.write(userLostKey, NullWritable.get());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
