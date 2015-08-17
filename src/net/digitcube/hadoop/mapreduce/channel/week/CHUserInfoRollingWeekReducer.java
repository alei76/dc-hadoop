package net.digitcube.hadoop.mapreduce.channel.week;

import java.io.IOException;
import java.util.Date;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoWeekRolling2;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHUserInfoRollingWeekReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	private Context context;

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		this.context = context;
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(),new Date());
		// 因为默认是结算程序运行的前一天，这里日期应该是每周末
		statDate = DateUtil.getStatWeekDate(context.getConfiguration()); 
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		
		// 本周信息
		String[] weekInfoArr = null;
		// 周滚存信息
		UserInfoWeekRolling2 userInfoWeekRolling = null;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			if (CHUserInfoRollingWeekMapper.DATA_FLAG_ROLL_INFO.equals(val.getSuffix())) {
				userInfoWeekRolling = new UserInfoWeekRolling2(scheduleTime, arr);
			} else if (CHUserInfoRollingWeekMapper.DATA_FLAG_WEEK_INFO.equals(val.getSuffix())) {
				weekInfoArr = arr;
			}
		}
		
		if(null == userInfoWeekRolling){
			userInfoWeekRolling = new UserInfoWeekRolling2(scheduleTime);
			int i = 0;
			userInfoWeekRolling.setAppId(key.getOutFields()[i++]);
			userInfoWeekRolling.setPlatform(key.getOutFields()[i++]);
			userInfoWeekRolling.setUid(key.getOutFields()[i++]);
		}
		
		if(null != weekInfoArr){
			int i = 0;
			String appId = weekInfoArr[i++];
			String appVer = weekInfoArr[i++];
			String platform = weekInfoArr[i++];
			String channel = weekInfoArr[i++];
			String country = weekInfoArr[i++];
			String province = weekInfoArr[i++];
			String uid = weekInfoArr[i++];
			String isFirstLogin = weekInfoArr[i++];
			String isFirstPay = weekInfoArr[i++];
			int weekLoginTimes = StringUtil.convertInt(weekInfoArr[i++],0);
			int weekOnlineTime = StringUtil.convertInt(weekInfoArr[i++],0);
			int weekOnlineDay = StringUtil.convertInt(weekInfoArr[i++],0);
			int weekPayAmount = StringUtil.convertInt(weekInfoArr[i++],0);
			int weekPayTimes = StringUtil.convertInt(weekInfoArr[i++],0);
			
			// 设置最大版本
			if(StringUtil.isEmpty(userInfoWeekRolling.getAppVer())
					|| appVer.compareTo(userInfoWeekRolling.getAppVer()) > 0){
				userInfoWeekRolling.setAppVer(appVer);
			}
			// 设置渠道
			userInfoWeekRolling.getPlayerWeekInfo().setChannel(channel);
			// 设置国家地区
			userInfoWeekRolling.setCountryAndProvince(country, province);
			
			// 设置首登周
			if (userInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate() == 0) {
				userInfoWeekRolling.getPlayerWeekInfo().setFirstLoginWeekDate(statDate);
			}
			// 设置最后登录周
			userInfoWeekRolling.getPlayerWeekInfo().setLastLoginWeekDate(statDate);
			
			if(weekPayAmount > 0){
				// 设置首次付费周
				if (userInfoWeekRolling.getPlayerWeekInfo().getFirstPayWeekDate() == 0) {
					userInfoWeekRolling.getPlayerWeekInfo().setFirstPayWeekDate(statDate);
				}
				// 设置最后一次次付费周
				userInfoWeekRolling.getPlayerWeekInfo().setLastPayWeekDate(statDate);
			}
		}
		
		// 记录登录
		userInfoWeekRolling.markLogin(statDate == userInfoWeekRolling.getPlayerWeekInfo().getLastLoginWeekDate());
		// 记录付费
		userInfoWeekRolling.markPay(statDate == userInfoWeekRolling.getPlayerWeekInfo().getLastPayWeekDate());
		
		// 留存统计
		statUserRetain(statDate, userInfoWeekRolling);
		// A. 统计流失漏斗
		//statUserLostFunnel(statDate, loginTimes, userInfoWeekRolling);
		// B. 流失
		//statUserLost(statDate, userInfoWeekRolling);
		// C. 回流
		//statUserBack(statDate, userInfoWeekRolling);
		
		// 输出滚存数据
		key.setOutFields(userInfoWeekRolling.toStringArray());
		key.setSuffix(Constants.SUFFIX_CHANNEL_ROLLING_WEEK);
		context.write(key, NullWritable.get());
	}

	private void statUserRetain(int statDate, UserInfoWeekRolling2 userInfoWeekRolling) 
			throws IOException, InterruptedException {
		
		// 统计日没有登录，直接返回
		if (statDate != userInfoWeekRolling.getPlayerWeekInfo().getLastLoginWeekDate()) {
			return;
		}
		
		String[] retain = new String[]{
				userInfoWeekRolling.getAppId(),
				userInfoWeekRolling.getAppVer(),
				userInfoWeekRolling.getPlatform(),
				userInfoWeekRolling.getPlayerWeekInfo().getChannel(),
				userInfoWeekRolling.getCountry(),
				userInfoWeekRolling.getProvince(),
				userInfoWeekRolling.getUid(),
				userInfoWeekRolling.getPlayerWeekInfo().getFirstLoginWeekDate()+"", // 用于判断新增或者活跃留存
				userInfoWeekRolling.getPlayerWeekInfo().getTrack()+"" //用户32周登录记录
		};
		
		keyObj.setOutFields(retain);
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_WEEK_RETAIN);
		context.write(keyObj, NullWritable.get());
	}
}
