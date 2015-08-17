package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class GuanKaStatReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	//进行过该任务的玩家帐号 ID(100w 个帐号 ID 约占 64MB 内存)
	private Set<String> taskStartAccountSet = new HashSet<String>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		taskStartAccountSet.clear();
		
		int totalPlayerNum = 0;
		int beginTimes = 0;
		int successTimes = 0;
		int failedTimes = 0;
		int failedExitTimes = 0;
		
		//关卡完成的总耗时(平均耗时需除以成功和失败的总次数)
		int totalDuration = 0;
		
		for(OutFieldsBaseModel val : values){
			String accountId = val.getOutFields()[0];
			String dataFlag = val.getOutFields()[1];
			String times = val.getOutFields()[2];
			
			taskStartAccountSet.add(accountId);
			if(Constants.DATA_FLAG_GUANKA_BEGIN_TIMES.equals(dataFlag)){
				beginTimes += StringUtil.convertInt(times, 0);
			}else if(Constants.DATA_FLAG_GUANKA_SUCCESS_TIMES.equals(dataFlag)){
				successTimes += StringUtil.convertInt(times, 0);
			}else if(Constants.DATA_FLAG_GUANKA_FAILED_TIMES.equals(dataFlag)){
				failedTimes += StringUtil.convertInt(times, 0);
			}else if(Constants.DATA_FLAG_GUANKA_FAILED_EXIT_TIMES.equals(dataFlag)){
				failedExitTimes += StringUtil.convertInt(times, 0);
			}else if(Constants.DATA_FLAG_GUANKA_DURATION.equals(dataFlag)){
				totalDuration += StringUtil.convertInt(times, 0);
			}
		}
		
		int totalTimes = successTimes+failedTimes;
		int avgDuration = 0 == totalTimes ? 0 : totalDuration/totalTimes;
		totalPlayerNum = taskStartAccountSet.size();
		String[] outFields = new String[]{
				totalPlayerNum+"",
				beginTimes+"",
				successTimes+"",
				failedTimes+"",
				failedExitTimes+"",
				avgDuration+""
		};
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_GUANKA_STAT);
		context.write(key, valObj);
	}
}
