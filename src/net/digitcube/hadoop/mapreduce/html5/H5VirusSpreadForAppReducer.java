package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class H5VirusSpreadForAppReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	private Set<String> ipSet = new HashSet<String>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		ipSet.clear();
		
		int childNodeCount = 0;
		int totalLoginTimes = 0;
		int totalOnlineTime = 0;
		int totalPVs = 0;
		
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			int i = 0;
			childNodeCount++;
			String loginTimes = arr[i++];
			String onlineTime = arr[i++];
			String pvs = arr[i++];
			String ipRecords = arr[i++];
			
			totalLoginTimes += StringUtil.convertInt(loginTimes ,0);
			totalOnlineTime += StringUtil.convertInt(onlineTime ,0);
			totalPVs += StringUtil.convertInt(pvs ,0);
			Set<String> set = StringUtil.getSetFromJson(ipRecords);
			if(null != set){
				ipSet.addAll(set);
			}
			
		}
		
		int uniqIps = ipSet.size();
		String[] valFields = new String[]{
				childNodeCount+"",
				totalLoginTimes+"",
				totalOnlineTime+"",
				totalPVs+"",
				uniqIps+""
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_VIRUS_SPREAD_FOR_APP);
		context.write(key, valObj);
	}
}
