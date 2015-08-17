package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5NewVirusSpreadForAppReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	private Set<String> ipSet = new HashSet<String>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		ipSet.clear();
		
		int childNodeCount = 0;
		int totalLoginTimes = 0;
		int totalOnlineTime = 0;
		int totalPVs = 0;
		String isOneLine = Constants.DATA_FLAG_NO;
		
		String platform = "";
		String H5_PromotionAPP = "";
		String H5_DOMAIN = "";
		String H5_REF = "";
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();			
			if(("ONELINE").equals(val.getSuffix())){
				if(null != arr[0] && "A".equals(arr[0])){
					isOneLine =  Constants.DATA_FLAG_YES ;
				}
			}else if(("VIRUS").equals(val.getSuffix())){		
				int i = 0;
				childNodeCount++;
				platform = arr[i++];
				H5_PromotionAPP = arr[i++];
				H5_DOMAIN = arr[i++];
				H5_REF = arr[i++];
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
		}
		
		//只有在线，没有病毒传播日志
		if(childNodeCount <= 0){
			return;
		}
		
		int uniqIps = ipSet.size();
		String[] valFields = new String[]{
				platform,
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				childNodeCount+"",
				totalLoginTimes+"",
				totalOnlineTime+"",
				totalPVs+"",
				uniqIps+"",
				isOneLine
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_NEW_VIRUS_SPREAD_FOR_APP);
		context.write(key, valObj);
	}
}
