package net.digitcube.hadoop.mapreduce;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 该 MR 实现两个功能
 * 1, 注册与激活玩家分离
 * 2, 对同一台设备的激活去重（同一台设备可能会上报多次）
 * 以 appid, platform, uid 为 key 
 *
 */
public class ActRegSeparateMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private Calendar calendar = null;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		calendar = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			// 默认取调度初始化时间的前一个小时
			calendar.setTime(date);
		}
		calendar.add(Calendar.HOUR_OF_DAY, -1);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog userInfoLog = new UserInfoLog(userInfoArr);
		int actTime = userInfoLog.getActTime();
		int regTime = userInfoLog.getRegTime();
		
		// 激活时间大于 0，视为激活玩家输出
		if(actTime > 0){ 
			//真实区服
			String[] keyFields = new String[]{
					userInfoLog.getAppID(),
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					userInfoLog.getGameServer()
			};
			
			keyObj.setSuffix(Constants.SUFFIX_ACT);
			keyObj.setOutFields(keyFields);
			//valObj.setOutFields(userInfoArr);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
			
			//全服
			String[] keyFields_AllGS = new String[] {
					userInfoLog.getAppID(),
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					MRConstants.ALL_GAMESERVER
			};
			keyObj.setOutFields(keyFields_AllGS);
			userInfoLog.setGameServer(MRConstants.ALL_GAMESERVER);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
		}
		
		// 注册时间大于 0，视为注册玩家输出
		if(regTime > 0){ 
			String[] keyFields = new String[]{
					userInfoLog.getAppID(),
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					userInfoLog.getGameServer()
			};
			
			keyObj.setSuffix(Constants.SUFFIX_REG);
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
			
			//全服
			String[] keyFields_AllGS = new String[] {
					userInfoLog.getAppID(),
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					MRConstants.ALL_GAMESERVER
			};
			keyObj.setOutFields(keyFields_AllGS);
			userInfoLog.setGameServer(MRConstants.ALL_GAMESERVER);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
		}
	}
}
