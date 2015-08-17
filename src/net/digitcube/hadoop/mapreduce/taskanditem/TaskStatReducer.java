package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class TaskStatReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	// 等级的统计不确定是否需要，暂时先保留
	private Map<String, Integer> beginLevelMap = new HashMap<String, Integer>();
	private Map<String, Integer> successLevelMap = new HashMap<String, Integer>();
	private Map<String, Integer> failedReasonMap = new HashMap<String, Integer>();

	// 进行过该任务的玩家帐号 ID(100w 个帐号 ID 约占 64MB 内存)
	private Set<String> taskStartAccountSet = new HashSet<String>();

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		beginLevelMap.clear();
		successLevelMap.clear();
		failedReasonMap.clear();
		taskStartAccountSet.clear();
		String[] keyArr = key.getOutFields();
		if ("ERROR".equals(keyArr[0])) {
			// error日志
			key.setSuffix(Constants.SUFFIX_ERROR_LOG);
			context.write(key, values.iterator().next());
			return;
		}
		int beginTimes = 0;
		int successTimes = 0;
		int failureTimes = 0;
		int avgTime = 0;
		int totalPlayerNum = 0;
		for (OutFieldsBaseModel val : values) {
			// accountId
			String accountId = val.getOutFields()[val.getOutFields().length - 1];
			taskStartAccountSet.add(accountId);

			// dataFlag
			String dataFlag = val.getOutFields()[0];
			if (TaskStatMapper.TASK_BEGIN.equals(dataFlag)) {
				beginTimes++;
				String level = val.getOutFields()[1];
				Integer count = beginLevelMap.get(level);
				if (null == count) {
					beginLevelMap.put(level, 1);
				} else {
					count++;
					beginLevelMap.put(level, count);
				}

			} else if (TaskStatMapper.TASK_SUCCESS.equals(dataFlag)) {
				successTimes++;

				String level = val.getOutFields()[1];
				Integer count = successLevelMap.get(level);
				if (null == count) {
					successLevelMap.put(level, 1);
				} else {
					count++;
					successLevelMap.put(level, count);
				}
				// 完成时间
				avgTime += StringUtil.convertInt(val.getOutFields()[2], 0);

			} else if (TaskStatMapper.TASK_FAILED.equals(dataFlag)) {
				failureTimes++;
				String reason = val.getOutFields()[1];
				Integer count = failedReasonMap.get(reason);
				if (null == count) {
					failedReasonMap.put(reason, 1);
				} else {
					count++;
					failedReasonMap.put(reason, count);
				}
			}
		}
		if (avgTime > 0) {
			avgTime = avgTime / successTimes;
		}

		totalPlayerNum = taskStartAccountSet.size();
		String[] outFields = new String[] { totalPlayerNum + "",
				beginTimes + "", successTimes + "", failureTimes + "",
				avgTime + "" };
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_TASK_STAT);
		context.write(key, valObj);

		key.setSuffix(Constants.SUFFIX_TASK_STAT_LAYOUT);
		/*
		 * 本快功能是 OK 的，只是现在没有需求故暂不输出 后面有需求时把注释打开即可
		 * 
		 * //任务开始等级分布 Set<Entry<String, Integer>> beginLevelSet =
		 * beginLevelMap.entrySet(); for(Entry<String, Integer> entry :
		 * beginLevelSet){ outFields = new String[]{
		 * Constants.DIMENSION_TASK_BEGIN_LEVEL, // type entry.getKey(), // vkey
		 * entry.getValue()+"" // val }; valObj.setOutFields(outFields);
		 * context.write(key, valObj); }
		 * 
		 * //任务完成等级分布 Set<Entry<String, Integer>> finishLevelSet =
		 * successLevelMap.entrySet(); for(Entry<String, Integer> entry :
		 * finishLevelSet){ outFields = new String[]{
		 * Constants.DIMENSION_TASK_FINISH_LEVEL, // type entry.getKey(), //
		 * vkey entry.getValue()+"" // val }; valObj.setOutFields(outFields);
		 * context.write(key, valObj); }
		 */

		// 任务失败原因分布
		Set<Entry<String, Integer>> failedReasonSet = failedReasonMap
				.entrySet();
		for (Entry<String, Integer> entry : failedReasonSet) {
			outFields = new String[] { Constants.DIMENSION_TASK_FAILED_REASON, // type
					entry.getKey(), // vkey
					entry.getValue() + "" // val
			};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}
	}
}
