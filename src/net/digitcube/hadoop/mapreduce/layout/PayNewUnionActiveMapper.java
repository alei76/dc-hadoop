package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑：
 * 用户当天的新增玩家、付费玩家 分别和活跃玩家关联
 * 得到新增玩家和付费玩家玩的在线信息(包括每次的登录时间及其在线时长)
 * 
 * 输入：
 * a) 新增玩家(online_first 原始日志)当天去重结果(依赖 @OnlineFirstDayMapper)
 * b) 活跃玩家(online 原始日志)当天去重结果(依赖 @OnlineDayMapper)
 * c) 付费玩家当天去重日志(依赖 @PaymentDayMapper 的结果)
 * 
 * Map 输出(通过 appId, accountID, platform)
 * key：appID, accountID, platform, gameServer
 * value：channel, onlineRecords
 * 
 */

public class PayNewUnionActiveMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	private String fileName;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		//if(fileName.contains(Constants.SUFFIX_ONLINE_HOUR)){
		if(fileName.contains(Constants.SUFFIX_ONLINE_DAY)){
			
			String appID = array[0];
			String platform = array[1];
			String accountID = array[2];
			String channel = array[3];
			String gameServer = array[7];
			//String loginTime = array[array.length - 3];
			//String onlineTime = array[array.length - 2];
			String onlineRecords = array[array.length - 1];
			
			writeMapUnionResult(context,
								appID, accountID,
								platform, channel, gameServer, onlineRecords,
								Constants.PLAYER_TYPE_ONLINE);
			
		}else if(fileName.contains(Constants.SUFFIX_PAYMENT_DAY)){
			
			PaymentDayLog paymentDayLog = new PaymentDayLog(array);
			
			String appID = paymentDayLog.getAppID();
			String platform = paymentDayLog.getPlatform();
			String accountID = paymentDayLog.getAccountID();
			String channel = paymentDayLog.getExtend().getChannel();
			String gameServer = paymentDayLog.getExtend().getGameServer();
			//String loginTime = "0";
			//String onlineTime = "0";
			String onlineRecords = "";
			
			writeMapUnionResult(context,
								appID, accountID,
								platform, channel, gameServer, onlineRecords,
								Constants.PLAYER_TYPE_PAYMENT);
			
		}else if(fileName.contains(Constants.LOG_FLAG_ONLINE_FIRST)){
			
			CommonHeader header = new CommonHeader(array);
			CommonExtend extend = new CommonExtend(array);
			
			String appID = header.getAppID();
			String platform = header.getPlatform();
			String accountID = header.getAccountID();
			String channel = extend.getChannel();
			String gameServer = extend.getGameServer();
			//String loginTime = "0";
			//String onlineTime = "0";
			String onlineRecords = "";
			
			writeMapUnionResult(context,
								appID, accountID,
								platform, channel, gameServer, onlineRecords,
								Constants.PLAYER_TYPE_NEWADD);
		}
	}
	
	/*private void writeMapUnionResult(Context context,
								   String appId, String accountId, 
								   String platform, String channel, String gameServer, String loginTime, String logoutTime,
								   String playerType) throws IOException, InterruptedException{
		
		String[] keyFields = new String[]{appId, accountId, platform};
		String[] valFields = new String[]{channel, gameServer, loginTime, logoutTime};
		
		mapKeyObj.setOutFields(keyFields);
		
		mapValObj.setOutFields(valFields);
		mapValObj.setSuffix(playerType);
		
		context.write(mapKeyObj, mapValObj);
	}*/
	
	private void writeMapUnionResult(Context context,
			   String appId, String accountId, 
			   String platform, String channel, String gameServer, String onlineRecords,
			   String playerType) throws IOException, InterruptedException{

		String[] keyFields = new String[]{appId, accountId, platform, gameServer};
		String[] valFields = new String[]{channel, onlineRecords};
		
		mapKeyObj.setOutFields(keyFields);
		
		mapValObj.setOutFields(valFields);
		mapValObj.setSuffix(playerType);
		
		context.write(mapKeyObj, mapValObj);
	}
}
