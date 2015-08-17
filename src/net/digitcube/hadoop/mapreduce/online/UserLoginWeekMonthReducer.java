package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.HadoopUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rickpan
 * @version 1.0 
 *          输入：上一自然周或自然月用户每天登录去重后的结果<br/>
 *          key:appid,platform,accountid<br>
 *          value:
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution,opersystem,brand,nettype,country,province,operators,
 *          lastlogintime(最后登陆时间),totalOnlineTime (总在线时长), maxLevel(最高等级),,
 *          totalLoginTimes(登陆总次数)<br>
 *          输出:<br/>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution,opersystem,brand,nettype,country,province,operators,
 *          lastlogintime(最后登陆时间),totalOnlineTime (总在线时长), maxLevel(最高等级),
 *          totalLoginTimes(登陆总次数)
 */

public class UserLoginWeekMonthReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		

		//最后登陆时间
		String lastLoginTime = "";
		//用户当天总在线时长
		int totalOnlineTime = 0;
		//用户的最高级别
		int maxLevel = 0;
		//用户登陆总次数
		int totalLoginTimes = 0;
		
		String[] defaultLastValue = null;
		for (OutFieldsBaseModel val : values) {
			
			defaultLastValue = val.getOutFields();
			String currentLastLoginTime = defaultLastValue[defaultLastValue.length - 4]; // loginTime
			int curreentOnlineTimes = StringUtil.convertInt(defaultLastValue[defaultLastValue.length - 3], 0);// onlineTime
			int currentLevel = StringUtil.convertInt(defaultLastValue[defaultLastValue.length - 2], 0);// level
			int currentLoginTimes = StringUtil.convertInt(defaultLastValue[defaultLastValue.length - 1], 0);// level

			lastLoginTime = lastLoginTime.compareTo(currentLastLoginTime)>0 ? lastLoginTime : currentLastLoginTime;
			totalOnlineTime += curreentOnlineTimes;
			maxLevel = maxLevel > currentLevel ? maxLevel : currentLevel;
			totalLoginTimes += currentLoginTimes;
		}
		
		/*
		 * 设置：
		 * 最后登陆时间
		 * 用户当天总在线时长
		 * 用户的最高级别
		 * 用户登陆总次数
		 */
		defaultLastValue[defaultLastValue.length - 4] = lastLoginTime;
		defaultLastValue[defaultLastValue.length - 3] = totalOnlineTime + "";
		defaultLastValue[defaultLastValue.length - 2] = maxLevel + "";
		defaultLastValue[defaultLastValue.length - 1] = totalLoginTimes + "";
		key.setOutFields(defaultLastValue);
		
		// 为Reduce日志加上后缀
		Configuration conf = context.getConfiguration();
		if(conf.getBoolean(Constants.KEY_ONLINE_NATURE_WEEK, false)){
			key.setSuffix(Constants.SUFFIX_ONLINE_NATURE_WEEK);
		}else if(conf.getBoolean(Constants.KEY_ONLINE_NATURE_MONTH, false)){
			key.setSuffix(Constants.SUFFIX_ONLINE_NATURE_MONTH);
		}else{
			HadoopUtil.throwWeekOrMonthNotSetExp(Constants.KEY_ONLINE_NATURE_WEEK,Constants.KEY_ONLINE_NATURE_MONTH);
		}
				
		context.write(key, NullWritable.get());
	}
}
