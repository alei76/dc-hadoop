package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 输入：
 * 当天设备激活日志  24 个时间片的激活日志(@ActRegSeparateMapper 的输出结果)
 * 当天的新增玩家去重日志(@OnlineFirstDayMapper 的输出结果)
 * 主要逻辑：
 * 两个输入以 appid + uid 进行为关联
 * reduce 端得到的输入为该台设备上的一个或多个新增用户
 * 当前业务认为同一台设备上多个新增用户只算一个，所以 reduce 端随机选择一个新增用户作为输出
 * 
 * 输入 1：激活设备日志(@ActRegSeparateMapper 24个时间片的计算结果-ACT)
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * ActTime,RegTime
 * 
 * 输入 2: 当天的新增玩家去重日志(@OnlineFirstDayMapper 的输出结果)
 * Timestamp,APPID,UID,AccountID,Platform,
 * Channel,AccountType,Gender,Age,GameServer,Resolution,OperSystem,Brand,NetType,Country,Province,Operators,
 * LoginTime,OnlineTime,Level
 * 
 * Map 输出：
 * key : appid, platform, uid
 * value : whole record
 * 
 */
public class ActDeviceNewPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private String dataFlag = "";
	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileName.contains(Constants.LOG_FLAG_ONLINE_FIRST)){
			dataFlag = Constants.DATA_FLAG_FIRST_ONLINE;
		}else if(fileName.contains(Constants.SUFFIX_ACT)){
			dataFlag = Constants.DATA_FLAG_DEVICE_ACT;
		}
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = new String[] {
			userInfoArr[Constants.INDEX_APPID], 
			userInfoArr[Constants.INDEX_PLATFORM],
			userInfoArr[Constants.INDEX_UID],
			userInfoArr[Constants.INDEX_GAMESERVER]
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(userInfoArr);
		mapValObj.setSuffix(dataFlag);
		
		context.write(mapKeyObj, mapValObj);
	}
}
