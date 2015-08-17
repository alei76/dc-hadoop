package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5NewPlayerInfoLayoutReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Set<String> ipSet = new HashSet<String>();
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values,
			Context context) throws IOException, InterruptedException {		
		
		if(Constants.SUFFIX_H5_NEW_LAYOUT_ON_DEVICE.equals(key.getSuffix())){
			int total = 0;
			for (OutFieldsBaseModel val : values) {
				String[] array = val.getOutFields();
				total += StringUtil.convertInt(array[0], 0);				
			}
			valObj.setOutFields(new String[]{total+""});
			context.write(key, valObj);
		}else if(Constants.SUFFIX_H5_NEW_ONLINE_PAY.equals(key.getSuffix())){
			ipSet.clear();
			int totalPlayer = 0;
			int totalLoginTimes = 0;
			int totalOnlineTime = 0;
			for (OutFieldsBaseModel val : values) {
				totalPlayer++;
				String[] arr = val.getOutFields();
				String loginTimes = arr[0];
				totalLoginTimes += StringUtil.convertInt(loginTimes, 0);
				String onlineTime = arr[1];
				totalOnlineTime += StringUtil.convertInt(onlineTime, 0);
				String ipRecords = arr[2];
				if(StringUtil.isEmpty(ipRecords)){
					ipRecords = "[]";
				}
				ipSet.addAll(StringUtil.getSetFromJson(ipRecords));
			}
			
			int uniqIpCount = ipSet.size();
			
			String[] valFields = new String[]{
					totalPlayer + "",
					totalLoginTimes + "",
					totalOnlineTime + "",
					uniqIpCount + ""
			}; 
			valObj.setOutFields(valFields);			
			context.write(key, valObj);			
		}		
	}
}
