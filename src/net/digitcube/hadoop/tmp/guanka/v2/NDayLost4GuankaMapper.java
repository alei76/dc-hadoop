package net.digitcube.hadoop.tmp.guanka.v2;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NDayLost4GuankaMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	//private static String helloKittyAppId = "56706B35787161602B8BA0A01FE5EF97";
	private static String helloKittyAppId = "8B71BBCFF7E33EE2479BF8926F3B1A16";
	static String SUFFIX_FLAG_NEW = "NEW";
	static String SUFFIX_FLAG_GUANKA = "GK";
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	String fileName = "";
	Date scheduleTime = null;
	int statDate = 0;
	int newAddDate = 0;
	int daysOffset = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		String targetAppId = context.getConfiguration().get("target.appid");
		//target.daysoffset=5
		daysOffset = context.getConfiguration().getInt("target.daysoffset", 0);
		if(StringUtil.isEmpty(targetAppId) || 0 == daysOffset){
			throw new RuntimeException("target.appid is not configed");
		}
		helloKittyAppId = targetAppId;
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		statDate = DateUtil.getStatDate(context.getConfiguration());
		newAddDate = statDate - 24 * 3600 * daysOffset;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		// 判断新增和流失
		if(fileName.contains(Constants.SUFFIX_USERROLLING)){
			UserInfoRollingLog log = new UserInfoRollingLog(scheduleTime, arr);
			String appId = log.getAppID();
			String accountId = log.getAccountID();
			String gameServer = log.getPlayerDayInfo().getGameRegion();
			boolean isNewAdd = newAddDate == log.getPlayerDayInfo().getFirstLoginDate();
			boolean isNDayLost = !log.isEverLogin(daysOffset);
			
			String pureAppId = appId.split("\\|")[0];
			if(pureAppId.equals(helloKittyAppId) 
					&& gameServer.equals(MRConstants.ALL_GAMESERVER)
					&& isNewAdd){
				mapKeyObj.setOutFields(new String[]{
						pureAppId,
						accountId
				});
				mapValObj.setSuffix(SUFFIX_FLAG_NEW);
				mapValObj.setOutFields(new String[]{
						isNewAdd ? "Y" : "N",
						isNDayLost ? "Y" : "N"
				});
				context.write(mapKeyObj, mapValObj);
			}
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
				String[] a = appId.split("\\|");
				String pureAppId = a[0];
				
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
