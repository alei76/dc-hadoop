package net.digitcube.hadoop.mapreduce.accountmonitor;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.model.UserInfoLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 主要逻辑：
 * 对于每台设备，算出该设备上注册的帐号数
 * 
 * 输入：
 * UserInfo 当天 24 个时间片的原始日志
 * 
 * Map
 * Key:	appid, platform, uid
 * Val: accountNum, channel, gameserver
 * 
 * 输出
 * appid, platform, channel, gameserver, max(accountNum)
 * 
 * 
 */
public class AccountNumPerDeviceMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog userInfoLog = new UserInfoLog(userInfoArr);
		if(userInfoLog.getRegTime() <= 0){
			return;
		}
		
		//真实区服
		String[] keyFields = new String[] {
				userInfoLog.getAppID(),
				userInfoLog.getPlatform(),
				userInfoLog.getUID(),
				userInfoLog.getGameServer()
		};
		
		String accountNum = 0 == userInfoLog.getAccountNum() ? "1" : userInfoLog.getAccountNum()+"";
		String[] valFields = new String[] {
				accountNum,
				userInfoLog.getChannel(), 
				userInfoLog.getGameServer()
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		
		context.write(mapKeyObj, mapValObj);
		
		// 全服
		String[] keyFields_AllGS = new String[]{
				userInfoLog.getAppID(),
				userInfoLog.getPlatform(),
				userInfoLog.getUID(),
				MRConstants.ALL_GAMESERVER
		};
		String[] valFields_AllGS = new String[] {
				accountNum,
				userInfoLog.getChannel(), 
				MRConstants.ALL_GAMESERVER
		};
		mapKeyObj.setOutFields(keyFields_AllGS);
		mapValObj.setOutFields(valFields_AllGS);
		context.write(mapKeyObj, mapValObj);
	}
}
