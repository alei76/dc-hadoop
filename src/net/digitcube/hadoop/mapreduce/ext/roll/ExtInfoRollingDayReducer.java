package net.digitcube.hadoop.mapreduce.ext.roll;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtInfoRollingDayReducer extends Reducer<OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, NullWritable>{

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	private Map<String, ItemInfo> itemDayMap = new HashMap<String, ItemInfo>();
	private Map<String, GuanKaInfo> guanKaDayMap = new HashMap<String, GuanKaInfo>();
	private Map<String, TaskInfo> taskDayMap = new HashMap<String, TaskInfo>();
	
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}


	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<BigFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		itemDayMap.clear();
		guanKaDayMap.clear();
		taskDayMap.clear();
		
		String maxVer = "";
		String tmpUid = "";
		String tmpChannel = "";
		ExtInfoRollingLog extInfoRollingLog = null;
		
		for (BigFieldsBaseModel val : values) {
			int i = 0;
			String[] arr = val.getOutFields();
			if(ExtInfoRollingDayMapper.DATA_FLAG_ROLLING.equals(val.getSuffix())){
				extInfoRollingLog = new ExtInfoRollingLog(arr);
				if(extInfoRollingLog.getAppVersion().compareTo(maxVer) > 0){
					maxVer = extInfoRollingLog.getAppVersion();
					tmpUid = extInfoRollingLog.getUid();
					tmpChannel = extInfoRollingLog.getChannel();
				}
			}else if(ExtInfoRollingDayMapper.DATA_FLAG_ITEM.equals(val.getSuffix())){
				String appVer = arr[i++];
				String uid = arr[i++];
				String channel = arr[i++];
				if(appVer.compareTo(maxVer) > 0){
					maxVer = appVer;
					tmpUid = uid;
					tmpChannel = channel;
				}
				String itemId = arr[i++];
				String itemType = arr[i++];
				int buyCount = StringUtil.convertInt(arr[i++],0);
				int getCount = StringUtil.convertInt(arr[i++],0);
				int useCount = StringUtil.convertInt(arr[i++],0);
				ItemInfo item = itemDayMap.get(itemId);
				if(null == item){
					item = new ItemInfo();
					item.setItemId(itemId);
					item.setItemType(itemType);
					itemDayMap.put(itemId, item);
				}
				item.setBuyCount(item.getBuyCount()+buyCount);
				item.setGetCount(item.getGetCount()+getCount);
				item.setUseCount(item.getUseCount()+useCount);
				
			}else if(ExtInfoRollingDayMapper.DATA_FLAG_GUANKA.equals(val.getSuffix())){
				String appVer = arr[i++];
				String uid = arr[i++];
				String channel = arr[i++];
				if(appVer.compareTo(maxVer) > 0){
					maxVer = appVer;
					tmpUid = uid;
					tmpChannel = channel;
				}
				String guanKaId = arr[i++];
				int beginTimes = StringUtil.convertInt(arr[i++],0);
				int successTimes = StringUtil.convertInt(arr[i++],0);
				int failureTimes = StringUtil.convertInt(arr[i++],0);
				
				GuanKaInfo guanKa = guanKaDayMap.get(guanKaId);
				if(null == guanKa){
					guanKa = new GuanKaInfo();
					guanKa.setGuanKaId(guanKaId);
					guanKaDayMap.put(guanKaId, guanKa);
				}
				guanKa.setBeginTimes(guanKa.getBeginTimes() + beginTimes);
				guanKa.setSuccessTimes(guanKa.getSuccessTimes() + successTimes);
				guanKa.setFailureTimes(guanKa.getFailureTimes() + failureTimes);
				
			}else if(ExtInfoRollingDayMapper.DATA_FLAG_TASK.equals(val.getSuffix())){
				String appVer = arr[i++];
				String uid = arr[i++];
				String channel = arr[i++];
				if(appVer.compareTo(maxVer) > 0){
					maxVer = appVer;
					tmpUid = uid;
					tmpChannel = channel;
				}
				String taskId = arr[i++];
				String taskType = arr[i++];
				int beginTimes = StringUtil.convertInt(arr[i++],0);
				int successTimes = StringUtil.convertInt(arr[i++],0);
				int failureTimes = StringUtil.convertInt(arr[i++],0);
				
				TaskInfo task = taskDayMap.get(taskId);
				if(null == task){
					task = new TaskInfo();
					task.setTaskId(taskId);
					task.setTaskType(taskType);
					taskDayMap.put(taskId, task);
				}
				task.setBeginTimes(task.getBeginTimes() + beginTimes);
				task.setSuccessTimes(task.getSuccessTimes() + successTimes);
				task.setFailureTimes(task.getFailureTimes() + failureTimes);
			}
		}
		
		if(null == extInfoRollingLog){
			extInfoRollingLog = new ExtInfoRollingLog();
			int i = 0;
			String appId = key.getOutFields()[i++];
			String platform = key.getOutFields()[i++];
			String gameServer = key.getOutFields()[i++];
			String accountId = key.getOutFields()[i++];
			extInfoRollingLog.setAppId(appId);
			extInfoRollingLog.setAppVersion(maxVer);
			extInfoRollingLog.setPlatform(platform);
			extInfoRollingLog.setChannel(tmpChannel);
			extInfoRollingLog.setGameServer(gameServer);
			extInfoRollingLog.setAccountId(accountId);
			extInfoRollingLog.setUid(tmpUid);
		}
		
		if(null != itemDayMap && !itemDayMap.isEmpty()){
			extInfoRollingLog.getItemInfoMap().put(statTime, itemDayMap);
		}
		if(null != guanKaDayMap && !guanKaDayMap.isEmpty()){
			extInfoRollingLog.getGuanKaInfoMap().put(statTime, guanKaDayMap);
		}
		if(null != taskDayMap && !taskDayMap.isEmpty()){
			extInfoRollingLog.getTaskInfoMap().put(statTime, taskDayMap);
		}
		
		//输出
		keyObj.setOutFields(extInfoRollingLog.toStringArray());
		keyObj.setSuffix(Constants.SUFFIX_EXT_INFO_ROLLING);
		context.write(keyObj, NullWritable.get());
	}	
}
