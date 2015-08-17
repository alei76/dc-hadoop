package net.digitcube.hadoop.mapreduce.ext;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskForPlayerReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		
		int beginTimes = 0;
		int successTimes = 0;
		int failureTimes = 0;
		
		String maxVer = ""; 
		String uid = null;
		String channel = null;
		String taskType = null; 
		
		for (OutFieldsBaseModel val : values) {
			int i = 0;
			String[] arr = val.getOutFields();
			String tmpVer = arr[i++];
			String tmpUid = arr[i++];
			String tmpChannel = arr[i++];
			String tmpTaskType = arr[i++];
			if(tmpVer.compareTo(maxVer) > 0){
				maxVer = tmpVer;
				uid = tmpUid;
				channel = tmpChannel;
				taskType = tmpTaskType;
			}
			
			String dataFlag = val.getSuffix();
			if (TaskForPlayerMapper.TASK_BEGIN.equals(dataFlag)) {
				beginTimes++;
			} else if (TaskForPlayerMapper.TASK_SUCCESS.equals(dataFlag)) {
				successTimes++;
			} else if (TaskForPlayerMapper.TASK_FAILED.equals(dataFlag)) {
				failureTimes++;
			}
		}

		int j = 0;
		String appId = key.getOutFields()[j++]; 
		String platform = key.getOutFields()[j++];
		String gameServer = key.getOutFields()[j++];
		String accountId = key.getOutFields()[j++];
		String taskId = key.getOutFields()[j++];
		String[] outFields = new String[] {
				appId,
				maxVer,
				platform,
				channel,
				gameServer,
				accountId,
				uid,
				taskId,
				taskType,
				beginTimes + "", 
				successTimes + "", 
				failureTimes + ""
		};
		key.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_TASK_FOR_PLAYER);
		context.write(key, NullWritable.get());
	}
}
