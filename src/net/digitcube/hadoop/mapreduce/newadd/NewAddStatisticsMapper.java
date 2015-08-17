package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.HeaderLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * @author 
 * 逻辑：
 * 新增激活设备、新增玩家、新增付费玩家、激活设备中新增玩家作为输入
 * 在 APPID,platform,channel,gameRegion 维度分别统计以上各项的玩家数
 * 
 * 输入:
 * 当天 24 个时间片的激活日志(依赖 @ActRegSeparateMapper 的结果)
 * 当天的新增玩家去重日志(@OnlineFirstDayMapper 的输出结果)
 * 当天 24 个时间片的新增付费玩家日志(payment_first)
 * 当天激活设备中新增玩家(依赖 @ActDeviceNewPlayerMapper 的结果)
 */
public class NewAddStatisticsMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private Text mapValObj = new Text();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		super.setup(context);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String dataFlag = "";
		//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileName.endsWith(Constants.SUFFIX_ACT)){
			
			dataFlag = Constants.DATA_FLAG_DEVICE_ACT;
		}else if(fileName.contains(Constants.LOG_FLAG_ONLINE_FIRST)){
			
			dataFlag = Constants.DATA_FLAG_FIRST_ONLINE;
		}else if(fileName.contains(Constants.LOG_FLAG_PAYMENT_FIRST)){
			
			dataFlag = Constants.DATA_FLAG_FIRST_PAY;
		}else if(fileName.endsWith(Constants.SUFFIX_ACT_DEV_FIR_ONLINE)){
			
			dataFlag = Constants.DATA_FLAG_AD_FONLINE;
		}else{
			return;
		}
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		HeaderLog headerLog = new HeaderLog(userInfoArr);
		
		//真实区服
		String[] keyFields = new String[] {	
				headerLog.getAppID(), 
				headerLog.getPlatform(),
				headerLog.getChannel(),
				headerLog.getGameServer()
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.set(dataFlag);
		context.write(mapKeyObj, mapValObj);
		
		// 全服
		// 其它所有输入在依赖 MR 中已按全服区服区分
		// 这里 payment first 是原始日志，需再按全服区服区分
		if(fileName.contains(Constants.LOG_FLAG_PAYMENT_FIRST)){
			//全服
			String[] keyFields_AllGS = new String[] {	
					headerLog.getAppID(), 
					headerLog.getPlatform(),
					headerLog.getChannel(),
					MRConstants.ALL_GAMESERVER
			};
			
			mapKeyObj.setOutFields(keyFields_AllGS);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
