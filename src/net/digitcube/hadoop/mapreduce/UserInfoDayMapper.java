package net.digitcube.hadoop.mapreduce;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：当天 24 个时间片的激活注册日志
 * 主要逻辑：
 * 对同一设备上玩家去重，同一台设备上可能存在多个玩家
 * 取最大的版本号、激活时间、注册时间、该设备上的帐号数
 *
 * 输出日志格式跟输出日志格式一样
 * 但输出结果激活时间、注册时间、该设备上的帐号数取的是最大值
 */
public class UserInfoDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog userInfoLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			userInfoLog = new UserInfoLog(userInfoArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(userInfoLog.getAppID())){
			return;
		}
		
		// 玩家注册登录前的数据以默认以 NO_LOGIN_ACCOUNTID 的身份上报
		// 而该玩家在 SDK 端的表现是激活和注册同时完成的，这就引起注册转化率达到 100% 的问题
		// 20141106 与彭俊约定，如果碰到是该玩家的注册日志，注册时间统统设为 0 
		if(userInfoLog.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)){
			userInfoLog.setRegTime(0);
		}
		
		int actTime = userInfoLog.getActTime();
		int regTime = userInfoLog.getRegTime();
		
		
		//碰到过同一台设备上同一天激活了两个版本
		//如果 appID 和版本号不分开统计的话，该设备将被重复计算
		//所以这里需把 appID 和版本号分开
		//在 reduce 中再取最大版本号（小版本号将被忽略）
		String[] arr = userInfoLog.getAppID().split("\\|");
		if(arr.length < 2){
			return;
		}
		String appID = arr[0];
		
		// 激活时间大于 0，视为激活玩家输出
		if(actTime > 0 || regTime > 0){ 
			//真实区服
			String[] keyFields = new String[]{
					appID,
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					userInfoLog.getGameServer()
			};
			
			//keyObj.setSuffix(Constants.SUFFIX_ACT);
			keyObj.setOutFields(keyFields);
			//valObj.setOutFields(userInfoArr);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
			
			//全服
			String[] keyFields_AllGS = new String[] {
					appID,
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
		/*if(regTime > 0){ 
			String[] keyFields = new String[]{
					appID,
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					userInfoLog.getGameServer()
			};
			
			//keyObj.setSuffix(Constants.SUFFIX_REG);
			//keyObj.setSuffix(Constants.SUFFIX_ACT);
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
			
			//全服
			String[] keyFields_AllGS = new String[] {
					appID,
					userInfoLog.getPlatform(),
					userInfoLog.getUID(),
					MRConstants.ALL_GAMESERVER
			};
			keyObj.setOutFields(keyFields_AllGS);
			userInfoLog.setGameServer(MRConstants.ALL_GAMESERVER);
			valObj.setOutFields(userInfoLog.toOldVersionArr());
			context.write(keyObj, valObj);
		}*/
	}
}
