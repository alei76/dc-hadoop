package net.digitcube.hadoop.mapreduce.channel.month;

import java.io.IOException;
import java.util.Date;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoMonthRolling2;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHUserInfoRollingMonthReducer extends
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
		statDate = DateUtil.getStatMonthDate(context.getConfiguration()); 
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		
		// 本周信息
		String[] monthInfoArr = null;
		// 周滚存信息
		UserInfoMonthRolling2 userInfoMonthRolling = null;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			if (CHUserInfoRollingMonthMapper.DATA_FLAG_ROLL_INFO.equals(val.getSuffix())) {
				userInfoMonthRolling = new UserInfoMonthRolling2(scheduleTime, arr);
			} else if (CHUserInfoRollingMonthMapper.DATA_FLAG_MONTH_INFO.equals(val.getSuffix())) {
				monthInfoArr = arr;
			}
		}
		
		if(null == userInfoMonthRolling){
			userInfoMonthRolling = new UserInfoMonthRolling2(scheduleTime);
			int i = 0;
			userInfoMonthRolling.setAppId(key.getOutFields()[i++]);
			userInfoMonthRolling.setPlatform(key.getOutFields()[i++]);
			userInfoMonthRolling.setUid(key.getOutFields()[i++]);
		}
		
		if(null != monthInfoArr){
			int i = 0;
			String appId = monthInfoArr[i++];
			String appVer = monthInfoArr[i++];
			String platform = monthInfoArr[i++];
			String channel = monthInfoArr[i++];
			String country = monthInfoArr[i++];
			String province = monthInfoArr[i++];
			String uid = monthInfoArr[i++];
			String isFirstLogin = monthInfoArr[i++];
			String isFirstPay = monthInfoArr[i++];
			int monthLoginTimes = StringUtil.convertInt(monthInfoArr[i++],0);
			int monthOnlineTime = StringUtil.convertInt(monthInfoArr[i++],0);
			int monthOnlineDay = StringUtil.convertInt(monthInfoArr[i++],0);
			int monthPayAmount = StringUtil.convertInt(monthInfoArr[i++],0);
			int monthPayTimes = StringUtil.convertInt(monthInfoArr[i++],0);
			
			// 设置最大版本
			if(StringUtil.isEmpty(userInfoMonthRolling.getAppVer())
					|| appVer.compareTo(userInfoMonthRolling.getAppVer()) > 0){
				userInfoMonthRolling.setAppVer(appVer);
			}
			// 设置渠道
			userInfoMonthRolling.getPlayerMonthInfo().setChannel(channel);
			// 设置国家地区
			userInfoMonthRolling.setCountryAndProvince(country, province);
			
			// 设置首登周
			if (userInfoMonthRolling.getPlayerMonthInfo().getFirstLoginMonthDate() == 0) {
				userInfoMonthRolling.getPlayerMonthInfo().setFirstLoginMonthDate(statDate);
			}
			// 设置最后登录周
			userInfoMonthRolling.getPlayerMonthInfo().setLastLoginMonthDate(statDate);
			
			if(monthPayAmount > 0){
				// 设置首次付费周
				if (userInfoMonthRolling.getPlayerMonthInfo().getFirstPayMonthDate() == 0) {
					userInfoMonthRolling.getPlayerMonthInfo().setFirstPayMonthDate(statDate);
				}
				// 设置最后一次次付费周
				userInfoMonthRolling.getPlayerMonthInfo().setLastPayMonthDate(statDate);
			}
		}
		
		// 记录登录
		userInfoMonthRolling.markLogin(statDate == userInfoMonthRolling.getPlayerMonthInfo().getLastLoginMonthDate());
		// 记录付费
		userInfoMonthRolling.markPay(statDate == userInfoMonthRolling.getPlayerMonthInfo().getLastPayMonthDate());
				
		// 留存统计
		statUserRetain(statDate, userInfoMonthRolling);
		// A. 统计流失漏斗
		//statUserLostFunnel(statDate, loginTimes, userInfoMonthRolling);
		// B. 流失
		//statUserLost(statDate, userInfoMonthRolling);
		// C. 回流
		//statUserBack(statDate, userInfoMonthRolling);
		
		// 输出滚存数据
		key.setOutFields(userInfoMonthRolling.toStringArray());
		key.setSuffix(Constants.SUFFIX_CHANNEL_ROLLING_MONTH);
		context.write(key, NullWritable.get());
	}

	private void statUserRetain(int statDate, UserInfoMonthRolling2 userInfoMonthRolling) 
			throws IOException, InterruptedException {
		
		// 统计日没有登录，直接返回
		if (statDate != userInfoMonthRolling.getPlayerMonthInfo().getLastLoginMonthDate()) {
			return;
		}
		
		String[] retain = new String[]{
				userInfoMonthRolling.getAppId(),
				userInfoMonthRolling.getAppVer(),
				userInfoMonthRolling.getPlatform(),
				userInfoMonthRolling.getPlayerMonthInfo().getChannel(),
				userInfoMonthRolling.getCountry(),
				userInfoMonthRolling.getProvince(),
				userInfoMonthRolling.getUid(),
				userInfoMonthRolling.getPlayerMonthInfo().getFirstLoginMonthDate()+"", // 用于判断新增或者活跃留存
				userInfoMonthRolling.getPlayerMonthInfo().getTrack()+"" //用户32周登录记录
		};
		
		keyObj.setOutFields(retain);
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_MONTH_RETAIN);
		context.write(keyObj, NullWritable.get());
	}
}
