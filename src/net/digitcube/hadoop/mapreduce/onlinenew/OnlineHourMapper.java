package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 主要逻辑：
 * 玩家在线日志中，对玩家的同一次登录，对不同的在线时间会有多次条日志
 * 该 MR 主要对同一玩家同一次登录的在线时间和级别做去重处理
 * 分别去最长在线时间和最大在线级别
 * 
 * 输入：用户每小时在线日志<br/>
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * LoginTime,OnlineTime,Level
 * 
 * 输出：<br/>
 * key	:	
 * APPID,Platform,AccountID,LoginTime
 * 
 * value(玩家一条完整在线日志):	
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * LoginTime,OnlineTime,Level
 * 
 */

public class OnlineHourMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(onlineArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}		
		
		//20141011
		//与彭俊约定，在线日志中只要 AccountId 是 _DESelf_DEFAULT_ACCOUNTID 的数据都过滤掉
		//这个是 SDK 中的一个 BUG
		if(onlineLog.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)){
			return;
		}
		
		//20141025
		//需要增加 SIM 卡运营商的统计，多输出 SIM 这个字段会破坏原有的协议
		//所以这里把 SIM 卡运营商和上报时运营商合到一起
		//在 OnlineDay 中再分别取到这两个值即可
		//这是临时解决方法，在重构中务必处理好扩展字段的情况
		String SimOpCode = onlineLog.getSimOpCode();
		if(null != SimOpCode && !"".equals(SimOpCode)){
			String operator = onlineLog.getOperators();//上报运营商
			onlineLog.setOperators(operator + MRConstants.DC_SEPARATOR + SimOpCode);
		}
		
		int onlineTime = onlineLog.getOnlineTime();
		// 20140402
		// 过滤掉在线时长大于 24 小时或者小于 0 的无效日志
		// 与高境、修强确认过：onlineTime 等于 0 视为有效数据
		/*
		 * 20140728 高境、修强再次约定修改：
		 * 对于异常在线时长数据不错过滤处理，只做兼容
		 * a)时长大于 1 天则取 1 天
		 * b)时长小于 0 则取 0 秒
		if(onlineTime >= 24*3600 || onlineTime < 0){
			return;
		}*/
		//a)时长大于 1 天则取 1 天
		//onlineTime = onlineTime > 24*3600 ? 24*3600 : onlineTime;
		//b)时长小于 0 则取 0 秒
		//onlineTime = onlineTime < 0 ? 0 : onlineTime;
		
		//20140729 : 高境约定修改
		//a)时长大于 1 天则取 1 秒
		onlineTime = onlineTime >= 24*3600 ? 1 : onlineTime;
		//b)时长小于等于 0 则取 1 
		onlineTime = onlineTime <= 0 ? 1 : onlineTime;
		
		onlineLog.setOnlineTime(onlineTime);
		
		//真实区服
		String[] keyFields = new String[] { 
				onlineLog.getAppID(),
				onlineLog.getPlatform(),
				onlineLog.getAccountID(),
				""+onlineLog.getLoginTime(),
				onlineLog.getGameServer()
		};
		//全服
		String[] keyFields_AllGS = new String[] { 
				onlineLog.getAppID(),
				onlineLog.getPlatform(),
				onlineLog.getAccountID(),
				""+onlineLog.getLoginTime(),
				MRConstants.ALL_GAMESERVER
		};
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(onlineLog.toOldVersionArr());
		context.write(mapKeyObj, mapValueObj);
		//全服
		onlineLog.setGameServer(MRConstants.ALL_GAMESERVER);
		mapValueObj.setOutFields(onlineLog.toOldVersionArr());
		mapKeyObj.setOutFields(keyFields_AllGS);
		context.write(mapKeyObj, mapValueObj);
	}
}
