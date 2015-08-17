package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import net.digitcube.hadoop.model.channel.OnlineLog2;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHOnlineDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private TreeMap<Integer, Integer> loginTimeMap = new TreeMap<Integer, Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		loginTimeMap.clear();
		
		OnlineLog2 tmpOnlineLog = null;
		for(OutFieldsBaseModel val : values){
			OnlineLog2 onlineLog = new OnlineLog2(val.getOutFields());
			Integer maxOnlineTime = loginTimeMap.get(onlineLog.getLoginTime());
			if(null == maxOnlineTime){
				loginTimeMap.put(onlineLog.getLoginTime(), onlineLog.getOnlineTime());
			}else if(onlineLog.getOnlineTime() > maxOnlineTime){
				loginTimeMap.put(onlineLog.getLoginTime(), onlineLog.getOnlineTime());
			}
			
			//取登录时间最早的值
			if(null == tmpOnlineLog || onlineLog.getLoginTime() < tmpOnlineLog.getLoginTime()){
				tmpOnlineLog = onlineLog;
			}
		}
		
		OnlineDayLog2 onlineDayLog = new OnlineDayLog2(tmpOnlineLog);
		int totalLoginTimes = loginTimeMap.size();
		String onlineRecords = StringUtil.getJsonStr(loginTimeMap);
		int totalOnlineTime = 0;
		for(int onlineTime : loginTimeMap.values()){
			totalOnlineTime += onlineTime;
		}
		
		onlineDayLog.totalLoginTimes = totalLoginTimes;
		onlineDayLog.totalOnlineTime = totalOnlineTime;
		onlineDayLog.onlineRecords = onlineRecords;
		
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_ONLINE);
		keyObj.setOutFields(onlineDayLog.toStringArr());
		context.write(keyObj, NullWritable.get());
	}
}
