package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;


public class H5OnlineDayForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Set<String> ipSet = new HashSet<String>();
	private Map<Integer, Integer> loginTimeMap = new HashMap<Integer, Integer>();


	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		ipSet.clear();
		loginTimeMap.clear();
		String isNewPlayer = Constants.DATA_FLAG_NO;
		String uid = null;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			int i = 0;
			String isNew = arr[i++];
			String ip = arr[i++];
			int loginTime = StringUtil.convertInt(arr[i++], 0);
			int onlineTime = StringUtil.convertInt(arr[i++], 0);
			String tmpUid = arr[i++];
			
			if(Constants.DATA_FLAG_YES.equals(isNew)){
				isNewPlayer = Constants.DATA_FLAG_YES;
			}
			
			if(null == uid){
				uid = tmpUid;
			}
			
			ipSet.add(ip);
			
			Integer oldOnlineTime = loginTimeMap.get(loginTime);
			if(null == oldOnlineTime){
				loginTimeMap.put(loginTime, onlineTime);
			}else if(onlineTime > oldOnlineTime){
				loginTimeMap.put(loginTime, onlineTime);
			}
		}
		
		int totalLoginTimes = loginTimeMap.size();
		int totalOnlineTime = 0;
		for(int onlineTime : loginTimeMap.values()){
			totalOnlineTime += onlineTime;
		}
		int uniqIpCount = ipSet.size();
		
		String onlineRecords = StringUtil.getJsonStr(loginTimeMap);
		String ipRecords = StringUtil.getJsonStr(ipSet);
		
		String[] valFields = new String[]{
				isNewPlayer + "",
				totalLoginTimes + "",
				totalOnlineTime + "",
				uniqIpCount + "",
				onlineRecords,
				ipRecords,
				uid
		};
		valObj.setOutFields(valFields);
		
		key.setSuffix(Constants.SUFFIX_H5_ONLINEDAY_FOR_PLAYER);
		context.write(key, valObj);
	}
}
