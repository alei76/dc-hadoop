package net.digitcube.hadoop.mapreduce.ext.roll;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.FieldValidationUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExtInfoRollingDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel>{

	static String DATA_FLAG_ROLLING = "R";
	static String DATA_FLAG_ITEM = "I";
	static String DATA_FLAG_GUANKA = "G";
	static String DATA_FLAG_TASK = "T";
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private BigFieldsBaseModel valObj = new BigFieldsBaseModel();
	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		//A. 昨天滚存
		if(fileSuffix.endsWith(Constants.SUFFIX_EXT_INFO_ROLLING)){
			ExtInfoRollingLog extInfo = new ExtInfoRollingLog(arr);
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(extInfo.getAppId())){
				return;
			}
			
			String[] keyFields = new String[]{
					extInfo.getAppId(), // appId without version
					extInfo.getPlatform(),
					extInfo.getGameServer(),
					extInfo.getAccountId()
			};
			
			String[] valFields = arr;
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_ROLLING);
			context.write(keyObj, valObj);
			
		//B. 道具购买/获得/消耗
		}else if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_FOR_PLAYER)){
			if(arr.length < 17){
				return;
			}
			int i = 0;			
			String appId = arr[i++];
			String appVer = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String uid = arr[i++];
			String playerType = arr[i++];
			String itemId = arr[i++];
			String itemType = arr[i++];
			String buyCount = arr[i++];
			String getCount = arr[i++];
			String useCount = arr[i++];
			String curTypeCntMap = arr[i++];
			String curTypeAmountMap = arr[i++];
			String getReasonCntMap = arr[i++];
			String useReasonCntMap = arr[i++];
			
			String[] keyFields = new String[]{
					appId, 
					platform,
					gameServer,
					accountId
			};
			
			String[] valFields = new String[]{
					appVer,
					uid,
					channel,
					itemId,
					itemType,
					buyCount,
					getCount,
					useCount
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_ITEM);
			context.write(keyObj, valObj);
			
		//D. 关卡
		}else if(fileSuffix.endsWith(Constants.SUFFIX_GUANKA_FOR_PLAYER)){
			if(arr.length < 16){
				return;
			}
			int i = 0;			
			String appId = arr[i++];
			String appVer = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String uid = arr[i++];
			String playerType = arr[i++];
			String guankaId = arr[i++];
			String beginTimes = arr[i++];
			String successTimes = arr[i++];
			String failedTimes = arr[i++];
			String failedExitTimes = arr[i++];
			String duration = arr[i++];
			String succDuration = arr[i++];
			String failDuration = arr[i++];
			
			String[] keyFields = new String[]{
					appId, // appId without version
					platform,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					appVer,
					uid,
					channel,
					guankaId,
					beginTimes,
					successTimes,
					failedTimes
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_GUANKA);
			context.write(keyObj, valObj);
		
		//E. 任务
		}else if(fileSuffix.endsWith(Constants.SUFFIX_TASK_FOR_PLAYER)){
			if(arr.length < 12){
				return;
			}
			int i = 0;
			String appId = arr[i++];
			String appVer = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String uid = arr[i++];
			String taskId = arr[i++];
			String taskType = arr[i++];
			String beginTimes = arr[i++];
			String successTimes = arr[i++];
			String failureTimes = arr[i++];
			
			String[] keyFields = new String[]{
					appId, // appId without version
					platform,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					appVer,
					uid,
					channel,
					taskId,
					taskType,
					beginTimes,
					successTimes,
					failureTimes
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_TASK);
			context.write(keyObj, valObj);
		}
	}
}
