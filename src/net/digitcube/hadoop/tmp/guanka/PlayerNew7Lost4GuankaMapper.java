package net.digitcube.hadoop.tmp.guanka;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PlayerNew7Lost4GuankaMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private static String helloKittyAppId = "56706B35787161602B8BA0A01FE5EF97";
	static String SUFFIX_FLAG_NEW = "NEW";
	static String SUFFIX_FLAG_NL7 = "NL7";
	static String SUFFIX_FLAG_GUANKA = "GK";
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		int i = 0;
		// 当天新增玩家
		if(fileName.contains(Constants.SUFFIX_NEWADD_NEWPAY_PLAYER)){
			String appId = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String isNewAddPlayer = arr[i++];
			String isNewPayPlayer = arr[i++];
			
			String pureAppId = appId.split("\\|")[0];
			if(pureAppId.equals(helloKittyAppId) && gameServer.equals(MRConstants.ALL_GAMESERVER)
					&& "Y".equals(isNewAddPlayer)){
				mapKeyObj.setOutFields(new String[]{
						pureAppId,
						accountId
				});
				mapValObj.setSuffix(SUFFIX_FLAG_NEW);
				mapValObj.setOutFields(new String[]{isNewAddPlayer});
				context.write(mapKeyObj, mapValObj);
			}
			
		// NL7 玩家
		}else if(fileName.contains(Constants.SUFFIX_USERFLOW)){
			String appId = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String userFlowType = arr[i++];
			String level = arr[i++];
			String accountId = arr[i++];

			String pureAppId = appId.split("\\|")[0];
			
			//只统计新增玩家 7 日流失
			if(pureAppId.equals(helloKittyAppId) && gameServer.equals(MRConstants.ALL_GAMESERVER)
					&& Constants.UserLostType.NewUserLost7.value.equals(userFlowType)){
				mapKeyObj.setOutFields(new String[]{
						pureAppId,
						accountId
				});
				mapValObj.setSuffix(SUFFIX_FLAG_NL7);
				mapValObj.setOutFields(new String[]{userFlowType});
				context.write(mapKeyObj, mapValObj);
			}
	
		// 当天关卡
		}else if(fileName.contains("EventSelf")){
			EventLog eventLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				eventLog = new EventLog(arr);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}
			
			String appId = eventLog.getAppID();
			String eventId = eventLog.getEventId();
			if(appId.contains(helloKittyAppId) && 
					(eventId.contains("DESelf_LevelsBegin") || eventId.contains("DESelf_LevelsEnd"))){
				
				String levelId = eventLog.getArrtMap().get("levelId");
				if(StringUtil.isEmpty(levelId)){
					return;
				}
				levelId = levelId.replace("关卡", "");
				String pureAppId = appId.split("\\|")[0];
				String accountId = eventLog.getAccountID();
				
				mapKeyObj.setOutFields(new String[]{
						pureAppId,
						accountId
				});
				mapValObj.setSuffix(SUFFIX_FLAG_GUANKA);
				mapValObj.setOutFields(new String[]{levelId});
				context.write(mapKeyObj, mapValObj);
			}
		}
	}
}
