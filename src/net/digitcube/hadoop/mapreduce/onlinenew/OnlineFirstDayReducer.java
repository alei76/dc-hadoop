package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlineFirstDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		//去重，取当天最后一个
		OnlineLog onlineLog = null;
		try{
			for(OutFieldsBaseModel val : values){
				if(null == onlineLog){
					onlineLog = new OnlineLog(val.getOutFields()); 
				}else{
					OnlineLog current = new OnlineLog(val.getOutFields());
					if(current.getLoginTime() > onlineLog.getLoginTime()){
						onlineLog = current; 
					}
				}
			}
		}catch(Exception e){
			return;
		}
		if(null == onlineLog){
			return;
		}
		
		//Added at 20140606 : iOS 渠道修正
		//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		//if(MRConstants.PLATFORM_iOS_STR.equals(onlineLog.getPlatform())){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(onlineLog.getAppID(), 
																onlineLog.getUID(), 
																onlineLog.getPlatform(), 
																onlineLog.getChannel());
			onlineLog.setChannel(reviseChannel);
		//}
		
		OutFieldsBaseModel randomOne = new OutFieldsBaseModel(onlineLog.toOldVersionArr());
		// 新增玩家去重
		// 为了让原有依赖 onlineFirsrt 原始日志的代码不改动
		// 这里输出后缀沿用原始日志的后缀，后续需改正过来
		randomOne.setSuffix(Constants.LOG_FLAG_ONLINE_FIRST);
		context.write(randomOne, NullWritable.get());
	}
}
