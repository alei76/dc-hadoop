package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class GuanKaForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	//关卡的开始次数、成功完成次数、失败次数
	private Map<String, Integer> beginTimesMap = new HashMap<String, Integer>();
	private Map<String, Integer> successTimesMap = new HashMap<String, Integer>();
	private Map<String, Integer> failedTimesMap = new HashMap<String, Integer>();
	//失败退出率(levelId, map(loginTime, timeResult))
	private Map<String, Map<Integer,TimeAndResult>> failedLogoutMap = new HashMap<String, Map<Integer,TimeAndResult>>();
	//关卡时长
	private Map<String, Integer> durationMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		beginTimesMap.clear();
		successTimesMap.clear();
		failedTimesMap.clear();
		failedLogoutMap.clear();
		durationMap.clear();
		
		//关卡完成的总耗时(不管关卡成功或失败)
		for(OutFieldsBaseModel val : values){
			String taskType = val.getOutFields()[0];
			String levelId = val.getOutFields()[1];
			if(Constants.DATA_FLAG_GUANKA_BEGIN.equals(taskType)){//begin times
				Integer count = beginTimesMap.get(levelId);
				if(null == count){
					beginTimesMap.put(levelId, 1);
				}else{
					beginTimesMap.put(levelId, count + 1);
				}
			}else if(Constants.DATA_FLAG_GUANKA_END.equals(taskType)){
				String result = val.getOutFields()[2];
				int endTime = StringUtil.convertInt(val.getOutFields()[3], 0);
				int loginTime = StringUtil.convertInt(val.getOutFields()[4], 0);
				int currentDuration = StringUtil.convertInt(val.getOutFields()[5], 0);
				
				//时长
				Integer totalDuration = durationMap.get(levelId);
				if(null == totalDuration){
					durationMap.put(levelId, currentDuration);
				}else{
					durationMap.put(levelId, currentDuration + totalDuration);
				}
				
				if(Constants.DATA_FLAG_EVENT_TRUE.equals(result)){
					Integer count = successTimesMap.get(levelId);
					if(null == count){
						successTimesMap.put(levelId, 1);
					}else{
						successTimesMap.put(levelId, count + 1);
					}
				}else if(Constants.DATA_FLAG_EVENT_FALSE.equals(result)){
					Integer count = failedTimesMap.get(levelId);
					if(null == count){
						failedTimesMap.put(levelId, 1);
					}else{
						failedTimesMap.put(levelId, count + 1);
					}
				}
				
				//loginTime,TimeAndResult
				Map<Integer,TimeAndResult> timeResultMap = failedLogoutMap.get(levelId);
				if(null == timeResultMap){
					timeResultMap = new HashMap<Integer,TimeAndResult>();
					timeResultMap.put(loginTime, new TimeAndResult(endTime, result));
					failedLogoutMap.put(levelId, timeResultMap);
				}else{
					TimeAndResult timeResult = timeResultMap.get(loginTime);
					if(null == timeResult){
						timeResultMap.put(loginTime, new TimeAndResult(endTime, result));
					}else{
						//每个 loginTime 只保存 endTime 最大的记录
						//若该记录为失败，则视为失败退出
						if(endTime > timeResult.getEndTime()){
							timeResult.setEndTime(endTime);
							timeResult.setResult(result);
						}
					}
				}
			}
		}
		
		try{
			writeResult(key, context);
		}catch(Throwable t){
			t.printStackTrace();
		}
	}
	
	private void writeResult(OutFieldsBaseModel key, Context context) throws IOException, InterruptedException{
		//beginTimes
		Set<String> levelIdSet = beginTimesMap.keySet();
		for(String levelId : levelIdSet){
			int beginTimes = beginTimesMap.get(levelId);
			String[] arr = levelId.split("\\|");
			String[] outFields = new String[]{
					arr[0], // level id
					arr[1], // seq no
					Constants.DATA_FLAG_GUANKA_BEGIN_TIMES,
					beginTimes + ""
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
			context.write(key, valObj);
		}
		
		//successTimes
		levelIdSet = successTimesMap.keySet();
		for(String levelId : levelIdSet){
			int successTime = successTimesMap.get(levelId);
			String[] arr = levelId.split("\\|");
			String[] outFields = new String[]{
					arr[0], // level id
					arr[1], // seq no
					Constants.DATA_FLAG_GUANKA_SUCCESS_TIMES,
					successTime + ""
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
			context.write(key, valObj);
		}
		
		//failedTimes
		levelIdSet = failedTimesMap.keySet();
		for(String levelId : levelIdSet){
			int failedTimes = failedTimesMap.get(levelId);
			String[] arr = levelId.split("\\|");
			String[] outFields = new String[]{
					arr[0], // level id
					arr[1], // seq no
					Constants.DATA_FLAG_GUANKA_FAILED_TIMES,
					failedTimes + "",
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
			context.write(key, valObj);
		}
		
		//failedExitTimes
		levelIdSet = failedLogoutMap.keySet();
		for(String levelId : levelIdSet){
			int failedExitTimes = 0;
			
			Map<Integer,TimeAndResult> timeResultMap = failedLogoutMap.get(levelId);
			Set<Entry<Integer,TimeAndResult>> loginTimeSet = timeResultMap.entrySet();
			//entry = loginTime, timeResult
			for(Entry<Integer,TimeAndResult> entry : loginTimeSet){
				//每个 loginTime 对应的最后记录若为失败，则视为失败退出
				TimeAndResult timeAndResult = entry.getValue();
				if(Constants.DATA_FLAG_EVENT_FALSE.equals(timeAndResult.getResult())){
					failedExitTimes++;
				}
			}
			
			String[] arr = levelId.split("\\|");
			String[] outFields = new String[]{
					arr[0], // level id
					arr[1], // seq no
					Constants.DATA_FLAG_GUANKA_FAILED_EXIT_TIMES,
					failedExitTimes + ""
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
			context.write(key, valObj);
		}
		
		//duration
		levelIdSet = durationMap.keySet();
		for(String levelId : levelIdSet){
			int duration = durationMap.get(levelId);
			String[] arr = levelId.split("\\|");
			String[] outFields = new String[]{
					arr[0], // level id
					arr[1], // seq no
					Constants.DATA_FLAG_GUANKA_DURATION,
					duration + "",
			};
			valObj.setOutFields(outFields);
			key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
			context.write(key, valObj);
		}
	}
	
	class TimeAndResult{
		int endTime;
		String result;
		
		private TimeAndResult(int endTime, String result) {
			this.endTime = endTime;
			this.result = result;
		}
		public int getEndTime() {
			return endTime;
		}
		public void setEndTime(int endTime) {
			this.endTime = endTime;
		}
		public String getResult() {
			return result;
		}
		public void setResult(String result) {
			this.result = result;
		}
	}
}
