package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatMapper;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.model.UserExtInfoRollingLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class UserExtInfoRollingDayMap extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel>{

	static String DATA_FLAG_DETAIL_ROLLING = "R";
	static String DATA_FLAG_ITEM_BUY = "IB";
	static String DATA_FLAG_ITEM_GET_USE = "IGU";
	static String DATA_FLAG_GUANKA = "GK";
	static String DATA_FLAG_TASK = "TASK";
	static Integer MAX_LENGTH = 65536;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
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
		if(fileSuffix.endsWith(Constants.SUFFIX_EXT_INFO_ROLL_DAY)){
			UserExtInfoRollingLog extInfoRollingLog = new UserExtInfoRollingLog(arr);
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(extInfoRollingLog.getAppId())){
				return;
			}
			
			String[] appInfo = extInfoRollingLog.getAppId().split("\\|");
			String[] keyFields = new String[]{
					appInfo[0], // appId without version
					extInfoRollingLog.getPlatform(),
					extInfoRollingLog.getGameServer(),
					extInfoRollingLog.getAccountID()
			};
			
			String[] valFields = new String[]{
					DATA_FLAG_DETAIL_ROLLING,
					appInfo[1], // appVersion without appId
					extInfoRollingLog.getChannel(),
					extInfoRollingLog.getDetailInfo()
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
			
		//B. 道具购买	
		}else if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_BUY_PLAYER)){
			int i = 0;			
			String appID = arr[i++];
			String platform = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String playerType = arr[i++];
			String channel = arr[i++];			
			String buyRecords = arr[i++];
			if(buyRecords.getBytes().length > MAX_LENGTH){
				return;
			}
			String firstLoginDate = arr[i++];
			String totalPayCur = arr[i++];
			
			String[] appInfo = appID.split("\\|");
			String[] keyFields = new String[]{
					appInfo[0], // appId without version
					platform,
					gameServer,
					accountId
			};
			
			String[] valFields = new String[]{
					DATA_FLAG_ITEM_BUY,
					appInfo[1], // appVersion without appId
					channel,	
					playerType,
					firstLoginDate,
					totalPayCur,
					buyRecords					
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
			
		//C. 道具获得与消耗
		}else if(fileSuffix.endsWith(Constants.SUFFIX_ITEM_GAINLOST_PLAYER)){
			if(arr.length != 10){
				return;
			}
			int i=0;
			
			String appID = arr[i++];
			String platform = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String playerType = arr[i++];			
			String channel = arr[i++];
			String itemGetRedords = arr[i++];
			String itemUseRedords = arr[i++];
			String firstLoginDate = arr[i++];
			String totalPayCur = arr[i++];		
		
			String[] appInfo = appID.split("\\|");
			String[] keyFields = new String[]{
					appInfo[0], // appId without version
					platform,
					gameServer,
					accountId
			};
			
			String[] valFields = new String[]{
					DATA_FLAG_ITEM_GET_USE,
					appInfo[1], // appVersion without appId
					channel,
					playerType,
					firstLoginDate,
					totalPayCur,
					itemGetRedords,
					itemUseRedords
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
		
		//D. 关卡
		}else if(fileSuffix.endsWith(Constants.SUFFIX_GUANKA_FOR_PLAYER)){
			if(arr.length < 9){
				return;
			}
			int i = 0;			
			
			String appID = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			
			String levelId = arr[i++];
			String seqno = arr[i++];
			String dataFlag = arr[i++];
			String times = arr[i++];
			
			String[] appInfo = appID.split("\\|");
			String[] keyFields = new String[]{
					appInfo[0], // appId without version
					platform,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					DATA_FLAG_GUANKA,
					appInfo[1], // appVersion without appId
					channel,
					levelId,
					seqno,
					dataFlag,
					times
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
		
		//E. 任务
		}else if(fileSuffix.endsWith(Constants.DESelf_TaskBegin)
				|| fileSuffix.endsWith(Constants.DESelf_TaskEnd)){
			
			String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = new EventLog(array);			
			
			String appID = eventLog.getAppID();
			
			//验证appId长度 并修正appId  add by mikefeng 20141031
			if(!FieldValidationUtil.validateAppIdLength(appID)){
				return;
			}
			
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			Map<String, String> map = eventLog.getArrtMap();
			String taskId = map.get("taskId");
			String taskType = map.get("taskType");
			// 由于 SDK 拼写错误的原因，这里需做兼容
			if (null == taskType) {
				taskType = map.get("taksType");
			}
			
			String[] appInfo = appID.split("\\|");
			String[] keyFields = new String[]{
					appInfo[0], // appId without version
					platform,
					gameServer,
					accountId
			};
			
			//任务开始
			String subDataFlag = TaskStatMapper.TASK_BEGIN;
			//任务结束
			if(fileSuffix.endsWith(Constants.DESelf_TaskEnd)){
				String result = map.get("result");
				if (Constants.DATA_FLAG_EVENT_TRUE.equals(result)) {// 成功
					subDataFlag = TaskStatMapper.TASK_SUCCESS;
				} else if (Constants.DATA_FLAG_EVENT_FALSE.equals(result)) {
					subDataFlag = TaskStatMapper.TASK_FAILED;
				}
			}
			String[] valFields = new String[]{
					DATA_FLAG_TASK,
					appInfo[1], // appVersion without appId
					channel,
					taskId,
					taskType,
					subDataFlag
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
			
			//全服
			//任务输入是原始数据，所以还需多输出全服统计
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			context.write(keyObj, valObj);
			
		}
	}
}
