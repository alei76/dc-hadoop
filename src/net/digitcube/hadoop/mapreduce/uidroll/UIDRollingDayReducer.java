package net.digitcube.hadoop.mapreduce.uidroll;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.Constants.UserLostType;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UIDRollingLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.IOSChannelUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UIDRollingDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	// 取结算日期(数据发生日期)
	private int statDate = 0;

	private Set<String> loginTimeSet = new HashSet<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		if(context.getConfiguration().getBoolean("is.hour.job", false)){
			//小时任务:结算时间取当天凌晨零点
			statDate = DateUtil.getStatDateForToday(context.getConfiguration());
		}else{
			//天任务:结算时间取昨天凌晨零点
			statDate = DateUtil.getStatDate(context.getConfiguration());
		}
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		loginTimeSet.clear();
		
		String[] onlineArray = null;
		String[] regArray = null;
		UIDRollingLog uidRollingLog = null;
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("R".equals(value.getSuffix())) {
				uidRollingLog = new UIDRollingLog(array);
			} else if ("O".equals(value.getSuffix())) {
				onlineArray = array;
				//loginTime
				loginTimeSet.add(onlineArray[onlineArray.length - 1]);
			} else if ("REG".equals(value.getSuffix())) {
				regArray = array;
			}
		}

		if(null == uidRollingLog){
			uidRollingLog = new UIDRollingLog();
			uidRollingLog.setAppID(key.getOutFields()[0]);
			uidRollingLog.setPlatform(key.getOutFields()[1]);
			uidRollingLog.setGameServer(key.getOutFields()[2]);
			uidRollingLog.setUid(key.getOutFields()[3]);
		}
		
		if(null != regArray){
			// APP 版本号：在线日志中版本号 > 滚存日志中版本号
			uidRollingLog.setVersion(regArray[0]);
			uidRollingLog.setChannel(regArray[1]);
			uidRollingLog.setAccountID(regArray[2]);
			
			//first login date
			if(0 == uidRollingLog.getFirstLoginDate()){
				uidRollingLog.setFirstLoginDate(statDate);
			}
			
			//last login date
			uidRollingLog.setLastLoginDate(statDate);
		}
		
		if(null != onlineArray){
			// APP 版本号：在线日志中版本号 > 滚存日志中版本号
			uidRollingLog.setVersion(onlineArray[0]);
			uidRollingLog.setChannel(onlineArray[1]);
			uidRollingLog.setAccountID(onlineArray[2]);
			
			//first login date
			if(0 == uidRollingLog.getFirstLoginDate()){
				uidRollingLog.setFirstLoginDate(statDate);
			}
			
			//last login date
			uidRollingLog.setLastLoginDate(statDate);
			uidRollingLog.setTotalOnlineDay(uidRollingLog.getTotalOnlineDay() + 1);
			uidRollingLog.setTotalLoginTimes(uidRollingLog.getTotalLoginTimes() + loginTimeSet.size());
			uidRollingLog.markLogin(true);
		}

		//修正渠道匹配
		String reviseChannel = IOSChannelUtil.checkForiOSChannel(uidRollingLog.getAppID(), 
				uidRollingLog.getUid(), 
				uidRollingLog.getPlatform(), 
				uidRollingLog.getChannel());
		uidRollingLog.setChannel(reviseChannel);
		
		//A. 统计新增设备
		statNewUIDs(statDate, uidRollingLog, context);
		//B. 统计玩家1,7,14,30 日留存
		// 1,7,14,30 日留存在 30 日留存中统计即可
		//statUIDStay(statDate, uidRollingLog, context);
		//C. 统计玩家流失漏斗
		statUID30dayStay(statDate, uidRollingLog, context);
		// D. 记录用户滚存记录
		key.setOutFields(uidRollingLog.toStringArray());
		key.setSuffix(Constants.SUFFIX_UID_ROLLING_DAY);
		context.write(key, NullWritable.get());
	}

	//统计新增设备数和活跃设备数
	private void statNewUIDs(int statDate, UIDRollingLog uidRollingLog, Context context) throws IOException, InterruptedException {
		if(statDate != uidRollingLog.getFirstLoginDate()){
			return;
		}
		OutFieldsBaseModel key = new OutFieldsBaseModel();
		key.setSuffix(Constants.SUFFIX_UID_NEWADD_DAY);
		key.setOutFields(new String[]{
				uidRollingLog.getAppID(),
				uidRollingLog.getVersion(),
				uidRollingLog.getPlatform(),
				uidRollingLog.getChannel(),
				uidRollingLog.getGameServer(),
				uidRollingLog.getUid(),
				uidRollingLog.getAccountID()
		});
		context.write(key, NullWritable.get());
	}
	
	// 统计设备 30 天留存
	private void statUID30dayStay(int statDate, UIDRollingLog uidRollingLog, Context context) throws IOException, InterruptedException {
		if (statDate != uidRollingLog.getLastLoginDate()) {// 统计日没有登录，直接返回
			return;
		}
		
		int i = 31;
		while ((--i) > 0) {
			int targetDate = statDate - i * 24 * 3600;
			if (uidRollingLog.getFirstLoginDate() == targetDate) { // 是否该天新增设备
				OutFieldsBaseModel key = new OutFieldsBaseModel();
				key.setSuffix(Constants.SUFFIX_UID_30DAY_RETAIN);
				key.setOutFields(new String[]{
						uidRollingLog.getAppID(),
						uidRollingLog.getVersion(),
						uidRollingLog.getPlatform(),
						uidRollingLog.getChannel(),
						uidRollingLog.getGameServer(),
						uidRollingLog.getUid(),
						uidRollingLog.getAccountID(),
						i+""
				});
				context.write(key, NullWritable.get());
			}
		}
	}
		
	//统计新增活跃设备的 1,7,14,30 留存
	private void statUIDStay(int statDate, UIDRollingLog uidRollingLog, Context context) throws IOException, InterruptedException {
		if (uidRollingLog.getLastLoginDate() == statDate) { //统计日有登陆过
			// 计算新增活跃玩家次日留存
			int targetDate = statDate - 24 * 3600;
			boolean isNewAddThatDay = targetDate == uidRollingLog.getFirstLoginDate();
			boolean isLoginThatDay = uidRollingLog.isLogin(targetDate, statDate);
			if (isNewAddThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.NewUserStay1, context);
			}
			if (isLoginThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.UserStay1, context);
			}
			
			// 计算新增活跃玩家7日留存
			targetDate = statDate - 7 * 24 * 3600;
			isNewAddThatDay = targetDate == uidRollingLog.getFirstLoginDate();
			isLoginThatDay = uidRollingLog.isLogin(targetDate, statDate);
			if (isNewAddThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.NewUserStay7, context);
			}
			if (isLoginThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.UserStay7, context);
			}
			
			// 计算付费用户14日留存
			targetDate = statDate - 14 * 24 * 3600;
			isNewAddThatDay = targetDate == uidRollingLog.getFirstLoginDate();
			isLoginThatDay = uidRollingLog.isLogin(targetDate, statDate);
			if (isNewAddThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.NewUserStay14, context);
			}
			if (isLoginThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.UserStay14, context);
			}
			
			// 计算付费用户30日留存
			targetDate = statDate - 30 * 24 * 3600;
			isNewAddThatDay = targetDate == uidRollingLog.getFirstLoginDate();
			isLoginThatDay = uidRollingLog.isLogin(targetDate, statDate);
			if (isNewAddThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.NewUserStay30, context);
			}
			if (isLoginThatDay) {
				writeUIDStay(uidRollingLog, UserLostType.UserStay30, context);
			}
		}
	}

	// 打印用户流动相关日志，流失/回流/留存
	private void writeUIDStay(UIDRollingLog uidRollingLog, Constants.UserLostType type, Context context) throws IOException, InterruptedException {
		OutFieldsBaseModel userLostKey = new OutFieldsBaseModel();
		userLostKey.setSuffix(Constants.SUFFIX_USERFLOW);
		userLostKey.setOutFields(new String[]{
				uidRollingLog.getAppID(),
				uidRollingLog.getVersion(),
				uidRollingLog.getPlatform(),
				uidRollingLog.getChannel(),
				uidRollingLog.getGameServer(),
				uidRollingLog.getUid(),
				uidRollingLog.getAccountID(),
				type.value
		});
		context.write(userLostKey, NullWritable.get());
	}
	
	private int getStatDate(Context context) {
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
}
