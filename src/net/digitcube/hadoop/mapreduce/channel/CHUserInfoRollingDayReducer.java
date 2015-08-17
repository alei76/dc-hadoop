package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import net.digitcube.hadoop.model.channel.UserInfoLog2;
import net.digitcube.hadoop.model.channel.UserInfoRollingLog2;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHUserInfoRollingDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	private int statDate = 0;

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;

	// 是否小时任务
	private boolean isHourJob = false;
	
	private Context context;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.context = context;
		isHourJob = context.getConfiguration().getBoolean("is.hour.job", false);
		if(isHourJob){
			statDate = DateUtil.getStatDateForToday(context.getConfiguration());
		}else{
			statDate = DateUtil.getStatDate(context.getConfiguration());
		}
		
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		UserInfoRollingLog2 userInfoRollingLog = null;
		OnlineDayLog2 onlineDayLog2 = null;
		UserInfoLog2 userInfoLog2 = null;
		
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if(CHUserInfoRollingDayMapper.DATA_FLAG_ROLLING.equals(value.getSuffix())){
				userInfoRollingLog = new UserInfoRollingLog2(scheduleTime, array);
			}else if(CHUserInfoRollingDayMapper.DATA_FLAG_ONLINE.equals(value.getSuffix())){
				onlineDayLog2 = new OnlineDayLog2(array);
			}else if(CHUserInfoRollingDayMapper.DATA_FLAG_INFO.equals(value.getSuffix())){
				userInfoLog2 = new UserInfoLog2(array);
			}
		}

		if(null == userInfoRollingLog){
			userInfoRollingLog = new UserInfoRollingLog2(scheduleTime);
			userInfoRollingLog.setAppId(key.getOutFields()[0]);
			userInfoRollingLog.setPlatform(key.getOutFields()[1]);
			userInfoRollingLog.setUid(key.getOutFields()[2]);
		}
		
		if(null == onlineDayLog2 && null != userInfoLog2){
			onlineDayLog2 = new OnlineDayLog2(userInfoLog2);
		}
		
		if (userInfoLog2 != null) {
			// 只取最早的激活和注册时间
			// 激活时间
			if (userInfoRollingLog.getPlayerDayInfo().getActTime() == 0) {
				userInfoRollingLog.getPlayerDayInfo().setActTime(userInfoLog2.getActTime());
			}
			// 注册时间
			if (userInfoRollingLog.getPlayerDayInfo().getRegTime() == 0) {
				userInfoRollingLog.getPlayerDayInfo().setRegTime(userInfoLog2.getActTime());
			}
			
			// 玩家注册时设置当天为首登日期
			if (userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {
				// 如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				userInfoRollingLog.getPlayerDayInfo().setFirstLoginDate(statDate);
			}

			// 记录渠道
			userInfoRollingLog.getPlayerDayInfo().setChannel(userInfoLog2.getChannel());
			// 记录版本号
			userInfoRollingLog.setAppVer(userInfoLog2.getAppVersion());
		}
		
		if (onlineDayLog2 != null) {
			if (userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate() == 0) {

				// 如果首登时间为 0 说明是新增玩家，用 statDate 作为首登日期
				// 首登日期
				userInfoRollingLog.getPlayerDayInfo().setFirstLoginDate(statDate);
				// 如果注册时间没设置 顺便设一下
				if (userInfoRollingLog.getPlayerDayInfo().getRegTime() == 0) {
					userInfoRollingLog.getPlayerDayInfo().setRegTime(statDate);
				}
			}
			// 记录版本
			userInfoRollingLog.setAppVer(onlineDayLog2.getAppVer());
			
			// 记录最后一次登录时间
			userInfoRollingLog.getPlayerDayInfo().setLastLoginDate(statDate);
						
			// 更新滚存信息
			userInfoRollingLog.getPlayerDayInfo().setAccountType(MRConstants.INVALID_PLACE_HOLDER_CHAR);
			userInfoRollingLog.getPlayerDayInfo().setBrand(
					onlineDayLog2.getExtendsHelper().getFieldsByKey(ExtendFieldsHelper.BRAND, "-"));
			userInfoRollingLog.getPlayerDayInfo().setCountry(
					onlineDayLog2.getExtendsHelper().getFieldsByKey(ExtendFieldsHelper.CNTY, "-"));
			
			userInfoRollingLog.getPlayerDayInfo().setOperators(
					onlineDayLog2.getExtendsHelper().getFieldsByKey(ExtendFieldsHelper.NETOP, "-"));
			userInfoRollingLog.getPlayerDayInfo().setOsVersion(
					onlineDayLog2.getExtendsHelper().getFieldsByKey(ExtendFieldsHelper.OS, "-"));
			userInfoRollingLog.getPlayerDayInfo().setProvince(
					onlineDayLog2.getExtendsHelper().getFieldsByKey(ExtendFieldsHelper.PROV, "-"));

			// 记录总在线时长
			int historyOnlineTime = userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime(); 
			userInfoRollingLog.getPlayerDayInfo().setTotalOnlineTime(historyOnlineTime 
					+ onlineDayLog2.totalOnlineTime);
			// 记录总登录次数
			int historyLoginTimes = userInfoRollingLog.getPlayerDayInfo().getTotalLoginTimes();
			userInfoRollingLog.getPlayerDayInfo().setTotalLoginTimes(historyLoginTimes
					+ onlineDayLog2.totalLoginTimes);
			// 记录渠道
			userInfoRollingLog.getPlayerDayInfo().setChannel(onlineDayLog2.getChannel());
			
			// 记录已玩天数
			userInfoRollingLog.getPlayerDayInfo().setTotalOnlineDay(
					userInfoRollingLog.getPlayerDayInfo().getTotalOnlineDay() + 1);
			// 记录周在线时长
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineTime(
					userInfoRollingLog.getPlayerDayInfo().getWeekOnlineTime()
							+ onlineDayLog2.totalOnlineTime);
			// 记录周登陆次数
			userInfoRollingLog.getPlayerDayInfo().setWeekLoginTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getWeekLoginTimes() + onlineDayLog2.totalLoginTimes));
			// 记录周已玩天数
			userInfoRollingLog.getPlayerDayInfo().setWeekOnlineDay(
					(byte) (userInfoRollingLog.getPlayerDayInfo()
							.getWeekOnlineDay() + 1));
			// 记录月在线时长
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineTime(
					userInfoRollingLog.getPlayerDayInfo().getMonthOnlineTime()
							+ onlineDayLog2.totalOnlineTime);
			// 记录月登陆次数
			userInfoRollingLog.getPlayerDayInfo().setMonthLoginTimes(
					(short) (userInfoRollingLog.getPlayerDayInfo()
							.getMonthLoginTimes() + onlineDayLog2.totalLoginTimes));
			// 记录月已玩天数
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineDay(
					(byte) (userInfoRollingLog.getPlayerDayInfo()
							.getMonthOnlineDay() + 1));
		}
		// 记录用户32天内的登陆情况
		userInfoRollingLog.markLogin(statDate == userInfoRollingLog.getPlayerDayInfo().getLastLoginDate());
					
		// A. 输出玩家数量及在线信息
		boolean isNewPlayer = statDate == userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		statOnlineInfo(statDate, onlineDayLog2, isNewPlayer);
		
		// B. 输出玩家留存及新鲜度情况
		if(!isHourJob){
			statUserRetain(statDate, userInfoRollingLog);
		}
		
		// G. 输出周玩家数据
		statUserWeek(statDate, userInfoRollingLog);
		// H. 输出月玩家数据
		statUserMonth(statDate, userInfoRollingLog);

		// I. 记录用户滚存记录
		key.setOutFields(userInfoRollingLog.toStringArray());
		key.setSuffix(Constants.SUFFIX_CHANNEL_ROLLING);
		context.write(key, NullWritable.get());
	}

	private void statOnlineInfo(int statDate, OnlineDayLog2 onlineDayLog2, boolean isNewPlayer) 
			throws IOException, InterruptedException {
		if(null == onlineDayLog2){
			return;
		}
		
		onlineDayLog2.setIsNewPlayer(isNewPlayer ? "Y" : "N");
		keyObj.setOutFields(onlineDayLog2.toStringArr());
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_ONLINE_INFO);
		context.write(keyObj, NullWritable.get());
	}
	
	// 统计用户流失漏斗
	private void statUserRetain(int statDate, UserInfoRollingLog2 userInfoRollingLog) 
			throws IOException, InterruptedException {
		
		// 统计日没有登录，直接返回
		if (statDate != userInfoRollingLog.getPlayerDayInfo().getLastLoginDate()) {
			return;
		}
		
		String[] retain = new String[]{
				userInfoRollingLog.getAppId(),
				userInfoRollingLog.getAppVer(),
				userInfoRollingLog.getPlatform(),
				userInfoRollingLog.getPlayerDayInfo().getChannel(),
				userInfoRollingLog.getPlayerDayInfo().getCountry(),
				userInfoRollingLog.getPlayerDayInfo().getProvince(),
				userInfoRollingLog.getUid(),
				userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate()+"", // 用于判断新增或者活跃留存
				userInfoRollingLog.getPlayerDayInfo().getTrack()+"" //用户32天登录记录
		};
		
		keyObj.setOutFields(retain);
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_RETAIN);
		context.write(keyObj, NullWritable.get());
	}
		
	// 输出周滚存数据
	private void statUserWeek(int statDate,
			UserInfoRollingLog2 userInfoRollingLog) throws IOException,
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
			int weekOnlineTime = userInfoRollingLog.getPlayerDayInfo().getWeekOnlineTime();
			byte weekOnlineDay = userInfoRollingLog.getPlayerDayInfo().getWeekOnlineDay();
			// 本周是否首次付费
			boolean isFirstPay = (statDate - userInfoRollingLog.getPlayerDayInfo().getFirstPayDate()) < 7 * 24 * 3600;
			// 本周是否首登（新增）
			boolean isFirstLogin = (statDate - userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate()) < 7 * 24 * 3600;
			// 本周付费金额及次数
			int weekPayAmount = userInfoRollingLog.getPlayerDayInfo().getWeekCurrencyAmount();
			int weekPayTimes = userInfoRollingLog.getPlayerDayInfo().getWeekPayTimes();
			String[] output = new String[] {
					userInfoRollingLog.getAppId(),
					userInfoRollingLog.getAppVer(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getCountry(),
					userInfoRollingLog.getPlayerDayInfo().getProvince(),
					userInfoRollingLog.getUid(),
					isFirstLogin ? "1" : "0",
					isFirstPay ? "1" : "0",
					weekLoginTimes + "",
					weekOnlineTime + "",
					weekOnlineDay + "",
					weekPayAmount + "",
					weekPayTimes + "" };
			OutFieldsBaseModel key = new OutFieldsBaseModel(output);
			key.setSuffix(Constants.SUFFIX_CHANNEL_WEEK_INFO);
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
			UserInfoRollingLog2 userInfoRollingLog) throws IOException,
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
					userInfoRollingLog.getAppId(),
					userInfoRollingLog.getAppVer(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel(),
					userInfoRollingLog.getPlayerDayInfo().getCountry(),
					userInfoRollingLog.getPlayerDayInfo().getProvince(),
					userInfoRollingLog.getUid(),
					isFirstLogin ? "1" : "0",
					isFirstPay ? "1" : "0",
					monthLoginTimes + "",
					monthOnlineTime + "",
					monthOnlineDay + "",
					userInfoRollingLog.getPlayerDayInfo().getMonthCurrencyAmount() + "",
					userInfoRollingLog.getPlayerDayInfo().getMonthPayTimes() + "" };
			OutFieldsBaseModel key = new OutFieldsBaseModel(output);
			key.setSuffix(Constants.SUFFIX_CHANNEL_MONTH_INFO);
			context.write(key, NullWritable.get());
			// 清零上月数据
			userInfoRollingLog.getPlayerDayInfo().setMonthLoginTimes((short) 0);
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineDay((byte) 0);
			userInfoRollingLog.getPlayerDayInfo().setMonthOnlineTime(0);
			userInfoRollingLog.getPlayerDayInfo().setMonthCurrencyAmount(0);
			userInfoRollingLog.getPlayerDayInfo().setMonthPayTimes((short) 0);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
