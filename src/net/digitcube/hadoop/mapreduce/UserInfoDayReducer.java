package net.digitcube.hadoop.mapreduce;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.IOSChannelUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserInfoDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		//去重，取最大激活时间注册时间
		UserInfoLog userInfoLog = null;
		for(OutFieldsBaseModel val : values){
			if(null == userInfoLog){
				userInfoLog = new UserInfoLog(val.getOutFields());
			}else{
				UserInfoLog current = new UserInfoLog(val.getOutFields());
				//取最大激活时间
				if(current.getActTime() > userInfoLog.getActTime()){
					userInfoLog.setActTime(current.getActTime());
				}
				//取非空 accountID
				if(!MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(current.getAccountID())
						&& !userInfoLog.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)){
					userInfoLog.setAccountID(current.getAccountID());
				}
				//取注册时间
				if(current.getRegTime() > userInfoLog.getRegTime()){
					userInfoLog.setRegTime(current.getRegTime());
					//取注册时间最大的 accountId
					userInfoLog.setAccountID(current.getAccountID());
				}
				//取该设备上的帐号数
				if(current.getAccountNum() > userInfoLog.getAccountNum()){
					userInfoLog.setAccountNum(current.getAccountNum());
				}
				
				//按字母自然顺序比较，取大的版本号
				if(current.getAppID().compareTo(userInfoLog.getAppID())>0){
					userInfoLog.setAppID(current.getAppID());
				}
			}
		}
		if(null == userInfoLog){
			return;
		}
		
		//Added at 20140606 : iOS 渠道修正
		//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		//if(MRConstants.PLATFORM_iOS_STR.equals(userInfoLog.getPlatform())){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(userInfoLog.getAppID(), 
																userInfoLog.getUID(), 
																userInfoLog.getPlatform(), 
																userInfoLog.getChannel());
			
			userInfoLog.setChannel(reviseChannel);
		//}
		
		OutFieldsBaseModel randomOne = new OutFieldsBaseModel(userInfoLog.toOldVersionArr());
		randomOne.setSuffix(Constants.SUFFIX_USERINFO_DAY);
		context.write(randomOne, NullWritable.get());
		
	}
}
