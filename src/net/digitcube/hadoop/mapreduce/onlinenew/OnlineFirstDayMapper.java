package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 主要功能：
 * Online_First 中可能存在重复的日志
 * 该 MR 以 appid,platform,accountId 为 key 对 Online_First 当天的原始日志做去重处理
 * 
 * 输入：Online_First 当天 24 个时间片的原始日志
 * 
 * map
 * key:
 * 		appid,platform,accountId
 * valur: 
 * 		原始日志
 * 
 * reduce:
 * 		去重后随机的一条日志
 *
 */
public class OnlineFirstDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(userInfoArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		// 以  appId,platform,accountId 为 key, 在 reduce 端去重
		//真实区服
		String[] keyFields = new String[]{
				onlineLog.getAppID(),
				onlineLog.getPlatform(),
				onlineLog.getAccountID(),
				onlineLog.getGameServer()
		};
		//全服
		String[] keyFields_AllGS = new String[]{
				onlineLog.getAppID(),
				onlineLog.getPlatform(),
				onlineLog.getAccountID(),
				MRConstants.ALL_GAMESERVER
		};
		
		keyObj.setOutFields(keyFields);
		//valObj.setOutFields(onlineLog.toOldVersionArr());
		valObj.setOutFields(userInfoArr);
		context.write(keyObj, valObj);
		//全服
		keyObj.setOutFields(keyFields_AllGS);
		userInfoArr[10] = MRConstants.ALL_GAMESERVER;
		valObj.setOutFields(userInfoArr);
		context.write(keyObj, valObj);
	}
}
