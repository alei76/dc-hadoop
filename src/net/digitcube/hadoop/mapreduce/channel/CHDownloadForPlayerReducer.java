package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHDownloadForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	private Map<String, Integer> rsDownloadMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		rsDownloadMap.clear();
		
		OnlineDayLog2 onlineDayLog2 = null;
		EventLog2 eventLog2 = null;
		
		int totalDownloadTimes = 0;
		for(OutFieldsBaseModel val : values){
			String[] paramArr = val.getOutFields();
			if(CHDownloadForPlayerMapper.DATA_FLAG_ONLINE.equals(val.getSuffix())){
				onlineDayLog2 = new OnlineDayLog2(paramArr);
				
			}else if(CHDownloadForPlayerMapper.DATA_FLAG_EVENT.equals(val.getSuffix())){
				EventLog2 tmpEventLog2 = new EventLog2(paramArr);
				//取时间最早的值作为默认值
				if(null == eventLog2 || tmpEventLog2.getTimestamp().compareTo(eventLog2.getTimestamp()) < 0){
					eventLog2 = tmpEventLog2;
				}
				
				String resId = tmpEventLog2.getAttrMap().get(AttrKey.RESID);
				if(!StringUtil.isEmpty(resId)){
					totalDownloadTimes++;
					Integer counter = rsDownloadMap.get(resId);
					if(null == counter){
						rsDownloadMap.put(resId, 1);
					}else{
						rsDownloadMap.put(resId, 1 + counter);
					}
				}
			}
		}
		
		if(null == eventLog2){
			return;
		}
		
		boolean isOnline = null != onlineDayLog2;
		String appId = isOnline ? onlineDayLog2.getAppId() : eventLog2.getAppID();
		String appVer = isOnline ? onlineDayLog2.getAppVer() : eventLog2.getAppVersion();
		String platform = isOnline ? onlineDayLog2.getPlatform() : eventLog2.getPlatform();
		String channel = isOnline ? onlineDayLog2.getChannel() : eventLog2.getChannel();
		String country = isOnline ? onlineDayLog2.getExtendsHelper().getCnty() : eventLog2.getExtendValue(ExtendKey.CNTY);
		String province = isOnline ? onlineDayLog2.getExtendsHelper().getProv() : eventLog2.getExtendValue(ExtendKey.PROV);
		String uid = isOnline ? onlineDayLog2.getUid() : eventLog2.getUID();
		String isNewPlayer = isOnline ? onlineDayLog2.getIsNewPlayer() : "N";
		String downloadTimes = totalDownloadTimes+"";
		String downloadRecords = StringUtil.getJsonStr(rsDownloadMap);
		
		String[] keyFields = new String[]{
				appId,
				appVer,
				platform,
				channel,
				country,
				province,
				uid,
				isNewPlayer,
				downloadTimes,
				downloadRecords
		};
		
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_D_FOR_PLAYER);
		context.write(keyObj, NullWritable.get());
	}
}
