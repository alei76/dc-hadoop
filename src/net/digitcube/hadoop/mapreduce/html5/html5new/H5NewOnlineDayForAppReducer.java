package net.digitcube.hadoop.mapreduce.html5.html5new;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5NewOnlineDayForAppReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Set<String> ipSet = new HashSet<String>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
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
		key.setSuffix(Constants.SUFFIX_H5_NEW_ONLINEDAY_FOR_APP);
		
		context.write(key, valObj);
	}
}
