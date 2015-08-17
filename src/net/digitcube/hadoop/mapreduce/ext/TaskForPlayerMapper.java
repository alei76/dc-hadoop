package net.digitcube.hadoop.mapreduce.ext;

import java.io.IOException;
import java.util.Map;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑 
 * 统计每玩家每任务的：开始次数、成功次数、失败次数
 * 
 * 输出:
 * appId, maxVer, platform, channel, gameServer, accountId, uid, 
 * taskId, taskType, beginTimes,  successTimes,  failureTimes
 */

public class TaskForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String TASK_BEGIN = "B";
	public static final String TASK_SUCCESS = "S";
	public static final String TASK_FAILED = "F";

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	String fileName = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = new EventLog(array);
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			String uid = eventLog.getUID();
			Map<String, String> map = eventLog.getArrtMap();
			String taskId = map.get("taskId");
			String taskType = map.get("taskType");
			// 由于 SDK 拼写错误的原因，这里需做兼容
			if (null == taskType) {
				taskType = map.get("taksType");
			}

			String[] appIdAndVer = StringUtil.getAppIdAndVer(appId);
			String pureAppId = appIdAndVer[0];
			String appVersion = appIdAndVer[1];
			
			// 真实区服
			String[] keyFields = new String[] {
					pureAppId, 
					platform,  
					gameServer, 
					accountId, 
					taskId,  
			};
			mapKeyObj.setOutFields(keyFields);
			
			String[] valFields = new String[] {
					appVersion, 
					uid, 
					channel, 
					taskType
			};
			mapValObj.setOutFields(valFields);
			

			if (fileName.contains(Constants.DESelf_TaskBegin)) {
				mapValObj.setSuffix(TASK_BEGIN);
			} else if (fileName.contains(Constants.DESelf_TaskEnd)) {
				String result = map.get("result");
				if (Constants.DATA_FLAG_EVENT_TRUE.equals(result)) {// 成功
					mapValObj.setSuffix(TASK_SUCCESS);
				} else if (Constants.DATA_FLAG_EVENT_FALSE.equals(result)) {
					mapValObj.setSuffix(TASK_FAILED);
				}
			}
			context.write(mapKeyObj, mapValObj);

			// 全服
			keyFields[2] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
		} catch (Throwable t) {}
	}
}
