package net.digitcube.hadoop.mapreduce.userroll.month;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants.UserLostType;
import net.digitcube.hadoop.mapreduce.domain.UserInfoMonthRolling;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年8月1日 下午2:42:15 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserInfoRollingMonthReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private Context context;
	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;

	private int monthDate_1 = 0;
	private int monthDate_2 = 0;
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.context = context;
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		statDate = getStatDate(); // 因为默认是结算程序运行的前一天，这里日期应该是每月末
		
		//20141226 用于判断流失/回流
		//月滚存每月 1 号调度执行，结算时间取上月最后一天
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.MONTH, -1); //月份减 1
		calendar.add(Calendar.DAY_OF_MONTH, -1); //天减1后为上月最后一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		monthDate_1 = (int)(calendar.getTimeInMillis() / 1000);
		
		
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.MONTH, -2); //月份减 2
		calendar.add(Calendar.DAY_OF_MONTH, -1); //天减1后为上月最后一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		monthDate_2 = (int)(calendar.getTimeInMillis() / 1000);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] userRollArray = null; // 历史滚存
		String[] userMonthInfoArray = null; // 本周用户详情

		//int statDate = getStatDate(); // 因为默认是结算程序运行的前一天，这里日期应该是每月末
		
		// APP 版本号
		// appVerArr[0] = 历史月滚存中版本号
		// appVerArr[1] = 日滚存日志中版本号
		// 滚存中取值优先级：日滚存日志中版本号 > 历史周滚存中版本号
		String newestVersion = null;
		String[] appVerArr = {null, null};
		
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("A".equals(array[0])) {
				userMonthInfoArray = array;
				
				//日滚存版本号
				appVerArr[1] = array[array.length - 1];
				
			} else if ("B".equals(array[0])) {
				userRollArray = array;
				
				//历史月滚存版本号
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
		// UserInfoMonthRolling userInfoMonthRolling = new
		// UserInfoMonthRolling();
		UserInfoMonthRolling userInfoMonthRolling = new UserInfoMonthRolling(scheduleTime);

		//userInfoMonthRolling.setAppID(keyArray[0]);
		userInfoMonthRolling.setAppID(keyArray[0] + "|" + newestVersion); //合并 appId 和 Version
		userInfoMonthRolling.setPlatform(keyArray[1]);
		userInfoMonthRolling.setAccountID(keyArray[2]);
		if (userRollArray != null) {
			userInfoMonthRolling.setInfoBase64(userRollArray[1]);
		}
		userInfoMonthRolling.getPlayerMonthInfo();
		// 处理上周滚存的信息
		int loginTimes = 0;
		int onlineTime = 0;
		int onlineDay = 0;
		int currencyAmount = 0;
		int payTimes = 0;
		if (userMonthInfoArray != null) {
			String channel = userMonthInfoArray[1];
			String gameRegion = userMonthInfoArray[2];
			boolean isFirstLogin = "1".equals(userMonthInfoArray[3]);
			boolean isFirstPay = "1".equals(userMonthInfoArray[4]);
			loginTimes = StringUtil.convertInt(userMonthInfoArray[5], 0);
			onlineTime = StringUtil.convertInt(userMonthInfoArray[6], 0);
			onlineDay = StringUtil.convertInt(userMonthInfoArray[7], 0);
			currencyAmount = StringUtil.convertInt(userMonthInfoArray[8], 0);
			payTimes = StringUtil.convertInt(userMonthInfoArray[9], 0);
			// 更新渠道信息
			userInfoMonthRolling.getPlayerMonthInfo().setChannel(channel);
			userInfoMonthRolling.getPlayerMonthInfo().setGameRegion(gameRegion);
			// 更新登陆信息
			if (userInfoMonthRolling.getPlayerMonthInfo()
					.getFirstLoginMonthDate() == 0) {
				if (isFirstLogin) {
					userInfoMonthRolling.getPlayerMonthInfo()
							.setFirstLoginMonthDate(statDate);
					userInfoMonthRolling.getPlayerMonthInfo()
							.setLastLoginMonthDate(statDate);
				}
			} else {
				userInfoMonthRolling.getPlayerMonthInfo()
						.setLastLoginMonthDate(statDate);
			}
			// 更新付费信息
			if (userInfoMonthRolling.getPlayerMonthInfo()
					.getFirstPayMonthDate() == 0) {
				if (isFirstPay) {
					userInfoMonthRolling.getPlayerMonthInfo()
							.setFirstPayMonthDate(statDate);
					userInfoMonthRolling.getPlayerMonthInfo()
							.setLastPayMonthDate(statDate);
				}
			} else {
				userInfoMonthRolling.getPlayerMonthInfo().setLastPayMonthDate(
						statDate);
			}
		}
		// 标记登陆
		userInfoMonthRolling.markLogin(loginTimes > 0);
		// 标记付费
		userInfoMonthRolling.markPay(currencyAmount > 0);
		// A. 统计流失漏斗
		statUserLostFunnel(statDate, loginTimes, userInfoMonthRolling);
		// B. 流失
		statUserLost(statDate, userInfoMonthRolling);
		// C. 回流
		statUserBack(statDate, userInfoMonthRolling);
		
		// 输出滚存数据
		key.setOutFields(userInfoMonthRolling.toStringArray());
		key.setSuffix(Constants.SUFFIX_USERROLLING_MONTH);
		context.write(key, NullWritable.get());
	}

	// 统计用户流失漏斗
	private void statUserLostFunnel(int statDate, int loginTimes,
			UserInfoMonthRolling userInfoMonthRolling) throws IOException,
			InterruptedException {
		if (loginTimes == 0) {
			// 统计日没有登录，直接返回
			return;
		}

		Calendar calendar = Calendar.getInstance();
		int i = 10;
		while ((--i) > 0) {
			// i月之前
			calendar.setTimeInMillis((long) statDate * 1000);
			calendar.add(Calendar.MONTH, -i);
			int targetDate = (int) (calendar.getTimeInMillis() / 1000);
			boolean isLogin = userInfoMonthRolling
					.isLogin(targetDate, statDate);
			if (!isLogin) // 是否该天活跃用户
				continue;
			// 是否该天新增用户
			boolean isNewUser = isMonthEqual(userInfoMonthRolling
					.getPlayerMonthInfo().getFirstLoginMonthDate(), targetDate);
			// 是否该天付费用户
			boolean isPayUser = userInfoMonthRolling
					.isPay(targetDate, statDate);
			OutFieldsBaseModel key = new OutFieldsBaseModel();
			key.setSuffix(Constants.SUFFIX_USER_LOST_FUNNEL_MONTH);
			if (isNewUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoMonthRolling,
						Constants.PLAYER_TYPE_NEWADD, i, loginTimes));
				context.write(key, NullWritable.get());
			}
			if (isPayUser) {
				key.setOutFields(getStardKeyWithArgs(userInfoMonthRolling,
						Constants.PLAYER_TYPE_PAYMENT, i, loginTimes));
				context.write(key, NullWritable.get());
			}
			key.setOutFields(getStardKeyWithArgs(userInfoMonthRolling,
					Constants.PLAYER_TYPE_ONLINE, i, loginTimes));
			context.write(key, NullWritable.get());
		}
	}

	/**
	 * oldLastLoginDate : 上次的最后登录日期
	 * 
	 * @param statDate
	 * @param oldLastLoginDate
	 * @param oldLastPayDate
	 * @param userInfoMonthRolling
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statUserBack(int statDate, UserInfoMonthRolling userInfoMonthRolling) 
			throws IOException, InterruptedException {
		//本月必须有登录才计算回流
		if (statDate == userInfoMonthRolling.getPlayerMonthInfo().getLastLoginMonthDate()) {
			// 上上月是否登录
			boolean isLoginMonthDate_2 = userInfoMonthRolling.isLogin(monthDate_2, statDate);
			// 上月是否登录
			boolean isLoginMonthDate_1 = userInfoMonthRolling.isLogin(monthDate_1, statDate);
			// 本月是否登录
			boolean isLoginMonthDate_0 = userInfoMonthRolling.isLogin(statDate, statDate);
			
			// 1 月回流条件：上上月有登录，上月无登录，本月有登录
			boolean isBackUser = isLoginMonthDate_2 && (!isLoginMonthDate_1) && isLoginMonthDate_0;
			
			// 统计新增付费活跃用户的 1 月回流
			if (isBackUser) {
				boolean isNewUser = monthDate_2 == userInfoMonthRolling.getPlayerMonthInfo().getFirstLoginMonthDate();
				boolean isPay = userInfoMonthRolling.isPay(monthDate_2, statDate);
				// 曾经付过费的玩家回流统计
				// 20140912 与 sandy 确认
				// 玩家只有付过费，并且是在回流日之前付的费才算是付费玩家回流
				// 如果玩家之前没有付过费，而在回流日付了费则不算是付费玩家回流
				boolean isEverPayBeforeBackDay = userInfoMonthRolling.getPlayerMonthInfo().getFirstPayMonthDate() > 0;
				
				writeUserFlowLog(monthDate_2, userInfoMonthRolling, Constants.PLAYER_TYPE_ONLINE, Constants.SUFFIX_USER_BACK_MONTH_1);
				if (isNewUser) {
					writeUserFlowLog(monthDate_2, userInfoMonthRolling, Constants.PLAYER_TYPE_NEWADD, Constants.SUFFIX_USER_BACK_MONTH_1);
				}
				if (isPay) {
					writeUserFlowLog(monthDate_2, userInfoMonthRolling, Constants.PLAYER_TYPE_PAYMENT, Constants.SUFFIX_USER_BACK_MONTH_1);
				}
				if (isEverPayBeforeBackDay) {
					writeUserFlowLog(monthDate_2, userInfoMonthRolling, Constants.PLAYER_TYPE_EVER_PAY, Constants.SUFFIX_USER_BACK_MONTH_1);
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
	private void statUserLost(int statDate, UserInfoMonthRolling userInfoMonthRolling) throws IOException,
			InterruptedException {
		//本月必须无登录才计算流失
		if (statDate != userInfoMonthRolling.getPlayerMonthInfo().getLastLoginMonthDate()) {
			//上月是否登录
			boolean isLoginMonthDate_1 = userInfoMonthRolling.isLogin(monthDate_1, statDate);
			//当月是否登录
			boolean isLoginMonthDate_0 = userInfoMonthRolling.isLogin(statDate, statDate);
			// 1 月流失条件：上月有登录，本月无登录
			if (isLoginMonthDate_1 && (!isLoginMonthDate_0)) {
				//是否上月新增
				boolean isNewUser = monthDate_1 == userInfoMonthRolling.getPlayerMonthInfo().getFirstLoginMonthDate();
				//上月是否付费
				boolean isPay = userInfoMonthRolling.isPay(monthDate_1, statDate);
				//是否曾经付费
				boolean isEverPay = userInfoMonthRolling.getPlayerMonthInfo().getFirstPayMonthDate() > 0;
				
				writeUserFlowLog(monthDate_1, userInfoMonthRolling, Constants.PLAYER_TYPE_ONLINE, Constants.SUFFIX_USER_LOST_MONTH_1);
				if (isNewUser) {
					writeUserFlowLog(monthDate_1, userInfoMonthRolling, Constants.PLAYER_TYPE_NEWADD, Constants.SUFFIX_USER_LOST_MONTH_1);
				}
				if (isPay) {
					writeUserFlowLog(monthDate_1, userInfoMonthRolling, Constants.PLAYER_TYPE_PAYMENT, Constants.SUFFIX_USER_LOST_MONTH_1);
				}
				if (isEverPay) {
					writeUserFlowLog(monthDate_1, userInfoMonthRolling, Constants.PLAYER_TYPE_EVER_PAY, Constants.SUFFIX_USER_LOST_MONTH_1);
				}
			}
		}
	}
	
	/**
	 * 把统计时间直接输出到结果中，入库是直接入前台表，不必再转换
	 * 如 1 月流失的 statTime 为上月，1 月回流的 statTime 为上上月
	 * @param statTime
	 * @param userInfoMonthRolling
	 * @param type
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeUserFlowLog(int statTime, UserInfoMonthRolling userInfoMonthRolling, 
			String playerType, String suffix) 
			throws IOException, InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(suffix);
		String[] keyFields = new String[]{
				statTime+"",
				userInfoMonthRolling.getAppID(),
				userInfoMonthRolling.getPlatform(),
				userInfoMonthRolling.getPlayerMonthInfo().getChannel(),
				userInfoMonthRolling.getPlayerMonthInfo().getGameRegion(),
				userInfoMonthRolling.getAccountID(),
				playerType
		};
		userLostKey.setOutFields(keyFields);
		context.write(userLostKey, NullWritable.get());
	}
	
	// 打印用户流动相关日志，流失/回流/留存
	private void writeUserFlowLog(UserInfoMonthRolling userInfoMonthRolling,
			Constants.UserLostType type) throws IOException,
			InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(Constants.SUFFIX_USERFLOW);
		userLostKey.setOutFields(getStardKeyWithArgs(userInfoMonthRolling,
				type.value, userInfoMonthRolling.getAccountID()));
		context.write(userLostKey, NullWritable.get());
	}
		
	private String[] getStardKeyWithArgs(
			UserInfoMonthRolling userInfoMonthRolling, Object... args) {
		List<String> list = new ArrayList<String>();
		list.add(userInfoMonthRolling.getAppID());
		list.add(userInfoMonthRolling.getPlatform());
		list.add(userInfoMonthRolling.getPlayerMonthInfo().getChannel());
		list.add(userInfoMonthRolling.getPlayerMonthInfo().getGameRegion());
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

	// 判断两个月份是否相等
	private boolean isMonthEqual(int date1, int date2) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long) date1);
		int year1 = calendar.get(Calendar.YEAR);
		int month1 = calendar.get(Calendar.MONTH);
		calendar.setTimeInMillis((long) date2);
		int year2 = calendar.get(Calendar.YEAR);
		int month2 = calendar.get(Calendar.MONTH);
		return year1 == year2 && month1 == month2;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
