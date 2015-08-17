package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class H5NewOnlineDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Set<String> ipSet = new HashSet<String>();
	private Map<Integer, Integer> loginTimeMap = new HashMap<Integer, Integer>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		ipSet.clear();
		loginTimeMap.clear();
		String platform = null;
		String H5_PromotionAPP = null;
		String H5_DOMAIN = null;
		String H5_REF = null;
		String H5_CRTIME = null;
		String UID = null;
		String accountType = null;
		String gender = null;
		String age = null;		
		String resolution = null;
		String opSystem = null;
		String brand = null;
		String netType = null;
		String country = null;
		String province = null;
		String operators = null;
		
		
		String[] keyArray = key.getOutFields();
		H5OnlineDayLog h5OnlineDayLog = new H5OnlineDayLog();
		h5OnlineDayLog.setAppId(keyArray[0]);		
		
		String maxVersion = "";
		int maxLevel = 0;
		int lastLoginTime = 0;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			
			int i = 0;
			platform = arr[i++];			
			H5_PromotionAPP = arr[i++];
			H5_DOMAIN = arr[i++];
			H5_REF = arr[i++];
			H5_CRTIME = arr[i++];
			UID = arr[i++];			
			String ip = arr[i++];
			int loginTime = StringUtil.convertInt(arr[i++], 0);
			lastLoginTime = lastLoginTime > loginTime ? lastLoginTime : loginTime;			
			int onlineTime = StringUtil.convertInt(arr[i++], 0);			
			ipSet.add(ip);			
			Integer oldOnlineTime = loginTimeMap.get(loginTime);
			if(null == oldOnlineTime){
				loginTimeMap.put(loginTime, onlineTime);
			}else if(onlineTime > oldOnlineTime){
				loginTimeMap.put(loginTime, onlineTime);
			}
			accountType = arr[i++];
			gender = arr[i++];
			age = arr[i++];
			resolution = arr[i++];
			opSystem = arr[i++];
			brand = arr[i++];
			netType = arr[i++];
			country = arr[i++];
			province = arr[i++];
			operators = arr[i++];
			int level = StringUtil.convertInt(arr[i++],0);
			maxLevel = maxLevel > level ? maxLevel : level;
			String currentVerion = arr[i++];			
			if(maxVersion.compareTo(currentVerion) < 0){
				maxVersion = currentVerion;
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
		
		
		//设置最大版本号
		h5OnlineDayLog.setAppId(h5OnlineDayLog.getAppId() + "|" + maxVersion);
		h5OnlineDayLog.setAccountId(keyArray[1]);	
		h5OnlineDayLog.setPlatform(platform);
		h5OnlineDayLog.setH5app(H5_PromotionAPP);
		h5OnlineDayLog.setH5domain(H5_DOMAIN);
		h5OnlineDayLog.setH5ref(H5_REF);
		h5OnlineDayLog.setH5crtime(StringUtil.convertInt(H5_CRTIME,0));
		h5OnlineDayLog.setUid(UID);
		h5OnlineDayLog.setTotalLoginTimes(totalLoginTimes);
		h5OnlineDayLog.setTotalOnlineTime(totalOnlineTime);
		h5OnlineDayLog.setUniqIpCount(uniqIpCount);
		h5OnlineDayLog.setOnlineRecords(onlineRecords);		
		h5OnlineDayLog.setIpRecords(ipRecords);				
		h5OnlineDayLog.setLastLoginTime(lastLoginTime);		
		h5OnlineDayLog.setAccountType(accountType);
		h5OnlineDayLog.setGender(gender);
		h5OnlineDayLog.setAge(age);
		h5OnlineDayLog.setResolution(resolution);
		h5OnlineDayLog.setOpSystem(opSystem);
		h5OnlineDayLog.setBrand(brand);
		h5OnlineDayLog.setNetType(netType);
		h5OnlineDayLog.setCountry(country);
		h5OnlineDayLog.setProvince(province);
		h5OnlineDayLog.setOperators(operators);		
		h5OnlineDayLog.setLevel(maxLevel);		

		if(ipRecords.getBytes("UTF-8").length > 65535){
			key.setSuffix("INVALID_DATE");
			key.setOutFields(h5OnlineDayLog.toStringArray());
			context.write(key, NullWritable.get());
		}else{
			key.setSuffix(Constants.SUFFIX_H5_NEW_ONLINEDAY);
			key.setOutFields(h5OnlineDayLog.toStringArray());
			context.write(key, NullWritable.get());
		}
	}
}
