package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TagInfoRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel> {

	public static final String DATA_FLAG_ROLL = "R";
	public static final String DATA_FLAG_ACTION = "A";
	public static final String DATA_FLAG_ONLINE = "O";
	public static final String DATA_FLAG_PAYMENT = "P";
	public static final String DATA_FLAG_PLAYER_LOST = "L";
	public static final String DATA_FLAG_ADD_TAG = "AT";
	public static final String DATA_FLAG_RM_TAG = "RT";
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private BigFieldsBaseModel valObj = new BigFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);

		if(fileSuffix.endsWith(Constants.SUFFIX_TAG_ROLLING_DAY)){
			TagInfoLog tagInfo = new TagInfoLog(paraArr);
			String[] keyFields = new String[]{
					tagInfo.getAppId(),
					tagInfo.getPlatform(),
					tagInfo.getGameServer(),
					tagInfo.getAccountId()
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(paraArr);
			valObj.setSuffix(DATA_FLAG_ROLL);
			context.write(keyObj, valObj);
			
		} else if (fileSuffix.contains(Constants.SUFFIX_ONLINE_DAY)) {
			
			OnlineDayLog onlineDayLog = new OnlineDayLog(paraArr);			
			String[] appInfo = onlineDayLog.getAppID().split("\\|");
			
			String[] keyFields = new String[] { 
					appInfo[0], 
					onlineDayLog.getPlatform(),
					onlineDayLog.getExtend().getGameServer(),
					onlineDayLog.getAccountID()
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(paraArr);
			valObj.setSuffix(DATA_FLAG_ONLINE);
			context.write(keyObj, valObj);
			
		} else if (fileSuffix.contains(Constants.SUFFIX_PAYMENT_DAY)) {
			PaymentDayLog paymentDayLog = new PaymentDayLog(paraArr);
			String[] appInfo = paymentDayLog.getAppID().split("\\|");
			
			String[] keyFields = new String[] { 
					appInfo[0], 
					paymentDayLog.getPlatform(),
					paymentDayLog.getExtend().getGameServer(),
					paymentDayLog.getAccountID()
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(paraArr);
			valObj.setSuffix(DATA_FLAG_PAYMENT);
			context.write(keyObj, valObj);
			
		}else if (fileSuffix.contains(Constants.SUFFIX_LOST_PLAYER_FOR_TAG)) {
			int i = 0;
			String appId = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String lostLevel = paraArr[i++];
			String lostDays = paraArr[i++];
			
			String[] appIdAndVer = appId.split("\\|");
			String[] keyFields = new String[]{
					appIdAndVer[0],
					platform,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					lostLevel,
					lostDays
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_PLAYER_LOST);
			context.write(keyObj, valObj);
			
		} else {
			
			EventLog eventLog = new EventLog(paraArr);
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			Map<String, String> map = eventLog.getArrtMap();
			
			String tagName = map.get("tag");
			if(StringUtil.isEmpty(tagName)){
				return;
			}
			String subTagName = map.get("subTag");
			String actionTime = map.get("startTime");
			actionTime = StringUtil.isEmpty(actionTime) ? MRConstants.INVALID_PLACE_HOLDER_NUM : actionTime;
			
			String[] appIdAndVer = appId.split("\\|");
			String pureAppId = appIdAndVer[0];
			String version = appIdAndVer[1];
			String[] keyFields = new String[]{
					pureAppId,
					platform,
					gameServer,
					accountId
			};
			
			String action = null;
			if(fileSuffix.endsWith(Constants.EVENT_ID_ADD_TAG)){
				action = DATA_FLAG_ADD_TAG;
			}else if(fileSuffix.endsWith(Constants.EVENT_ID_RM_TAG)){
				action = DATA_FLAG_RM_TAG;
			}
			subTagName = StringUtil.isEmpty(subTagName) ? MRConstants.INVALID_PLACE_HOLDER_CHAR : subTagName;
			String[] valFields = new String[]{
					version,
					channel,
					tagName,
					subTagName,
					action,
					actionTime
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			
			valObj.setSuffix(DATA_FLAG_ACTION);
			context.write(keyObj, valObj);
			
			// 全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			
			valObj.setSuffix(DATA_FLAG_ACTION);
			context.write(keyObj, valObj);
		}
	}
}
