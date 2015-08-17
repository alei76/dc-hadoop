package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑 对任务进行统计：开始次数、完成次数、失败次数 开始和完成在等级上的分布，失败次数在原因上的分布
 * 
 * Map: key： APPID,Platform,Channel,GameServer,taskId,taskType value： if begin
 * value=[begin,level] else if complete value=[end,level,time] else if failed
 * value=[failed,reason]
 * 
 * 
 * Reduce: 
 * a) 任务开始结束
 * APPID,Platform,Channel, GameServer,taskId,taskType,beginTimes,completeTimes ,failedTimes,completeAVGTime
 * b) 任务失败原因分布 
 * APPID,Platform,Channel, GameServer,taskId,taskType,failed, reason1, times
 * c) 任务开始结束在等级上的分布（已实现，未输出） 
 * APPID,Platform,Channel, GameServer,taskId,taskType,begin, level1, times 
 * APPID,Platform,Channel, GameServer,taskId,taskType,end, level1, times
 */

public class TaskStatMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String TASK_BEGIN = "B";
	public static final String TASK_SUCCESS = "S";
	public static final String TASK_FAILED = "F";

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	String fileName = "";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = new EventLog(array);
			String appID = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String accountId = eventLog.getAccountID();
			int duration = eventLog.getDuration();
			Map<String, String> map = eventLog.getArrtMap();
			String taskId = map.get("taskId");
			String taskType = map.get("taskType");
			String level = map.get("level");
			// 由于 SDK 拼写错误的原因，这里需做兼容
			if (null == taskType) {
				taskType = map.get("taksType");
			}
			if (null == level) {
				level = "1";
			}

			// 真实区服
			String[] keyFields = new String[] { appID, platform, channel,
					gameServer, taskId, taskType };
			String[] valFields = null;

			if (fileName.contains(Constants.DESelf_TaskBegin)) {
				valFields = new String[] { TASK_BEGIN, level, accountId };
			} else if (fileName.contains(Constants.DESelf_TaskEnd)) {
				String result = map.get("result");
				if (Constants.DATA_FLAG_EVENT_TRUE.equals(result)) {// 成功
					valFields = new String[] { TASK_SUCCESS, level,
							duration + "", accountId };
				} else if (Constants.DATA_FLAG_EVENT_FALSE.equals(result)) {
					String reason = map.get("reason");
					valFields = new String[] { TASK_FAILED, reason, accountId };
				}
			}

			if (null == valFields) {
				return;
			}
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);

			// 全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
		} catch (Throwable t) {
			long timestamp = System.currentTimeMillis();
			mapKeyObj.setOutFields(new String[] { "ERROR", timestamp + "" });
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			t.printStackTrace(pw);
			mapValObj.setOutFields(new String[] {
					value.toString(),
					sw.toString().replaceAll("\t", "/t")
							.replaceAll("\r\n", "/r/n").replaceAll("\r", "/r")
							.replaceAll("\n", "/n") });
			context.write(mapKeyObj, mapValObj);
		}

	}

	public final static void main(String[] args) {

	}
}
