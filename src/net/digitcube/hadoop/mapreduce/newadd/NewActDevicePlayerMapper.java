package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NewActDevicePlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);

		if(fileName.endsWith(Constants.SUFFIX_NEW_ACTDEVICE_PLAYER)){
			/* 20140905 : 这部分逻辑直接从 UserInfoDay 中计算(移到最下面)
			 * 注意：目前 UserInfoDay 的计算是按 UID 去重的，后面会修改为按 accountId 去重
			 * 
			String accountNum = arr[5];
			int uidNums = arr[6].split(",").length;
			String isNewAddPlayer = arr[7];
			
			valFields = new String[] {Constants.DATA_FLAG_NEWACT_DEVICE, uidNums+""};
			mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
			
			// 如果是新增玩家，则输出单台设备帐号数统计
			if(Constants.DATA_FLAG_YES.equals(isNewAddPlayer)){
				//激活设备中新增玩家
				valFields= new String[] {Constants.DATA_FLAG_ACTDEV_PLAYER, "1"};
				mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				
				//单台设备帐号数分布统计
				int accNum = (0 == StringUtil.convertInt(accountNum, 0)) ? 1 : StringUtil.convertInt(accountNum, 0);
				int accountNumRange = EnumConstants.getAccNumPerDevRange(accNum);
				
				String[] accNumPerDev = new String[]{
						appId, platform, channel, gameServer, 
						Constants.PLAYER_TYPE_NEWADD, 
						Constants.DIMENSION_DEVICE_ACC_NUM_PER_DEV, 
						accountNumRange+""
				};
				
				mapKeyObj.setSuffix(Constants.SUFFIX_ACCOUNT_NUM_PER_DEV);
				mapKeyObj.setOutFields(accNumPerDev);
				context.write(mapKeyObj, mapValObj);
			}*/
		}else if(fileName.endsWith(Constants.SUFFIX_NEWADD_NEWPAY_PLAYER)){
			String appId = arr[0];
			String platform = arr[1];
			String channel = arr[2];
			String gameServer = arr[3];
			String accountId = arr[4];
			String isNewAddPlayer = arr[5];
			String isNewPayPlayer = arr[6];
			
			channel = channel.trim();
			gameServer = gameServer.trim();
			
			String[] keyFields = new String[] {appId, platform, channel, gameServer};
			String[] valFields = null;
			if(Constants.DATA_FLAG_YES.equals(isNewAddPlayer)){
				valFields = new String[] {Constants.DATA_FLAG_NEWADD_PLAYER, "1"};
				mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
			}
			
			if(Constants.DATA_FLAG_YES.equals(isNewPayPlayer)){
				valFields = new String[] {Constants.DATA_FLAG_NEWPAY_PLAYER, "1"};
				mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
			}
		}else if(fileName.endsWith(Constants.SUFFIX_USERINFO_DAY)){
			/* 20140905 : 激活设备数、激活设备中的新增玩家、小号分析统计直接从 UserInfoDay 输出中计算
			 * 注意：目前 UserInfoDay 的计算是按 UID 去重的，后面会修改为按 accountId 去重
			 * 如果按 accountId 去重的话，本段逻辑讲需要一起修改
			 */
			UserInfoLog userInfoLog = new UserInfoLog(arr);
			
			userInfoLog.setChannel(userInfoLog.getChannel().trim());
			userInfoLog.setGameServer(userInfoLog.getGameServer().trim());
			
			String[] keyFields = new String[] {
					userInfoLog.getAppID(), 
					userInfoLog.getPlatform(), 
					userInfoLog.getChannel(), 
					userInfoLog.getGameServer()
			};
			String[] valFields = null;
			
			//A. 激活设备数统计
			if(userInfoLog.getActTime() > 0){
				valFields = new String[] {Constants.DATA_FLAG_NEWACT_DEVICE, "1"};
				mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
			}
			//B. 激活设备中的新增玩家统计
			if(userInfoLog.getActTime() > 0 && userInfoLog.getRegTime() > 0){
				//激活设备中新增玩家
				valFields= new String[] {Constants.DATA_FLAG_ACTDEV_PLAYER, "1"};
				mapKeyObj.setSuffix(Constants.SUFFIX_NEW_ADD_DEV_PLAYER);
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
			}
			//C. 小号统计（只有注册的时候才做小号统计）
			if(userInfoLog.getRegTime() > 0){
				//单台设备帐号数分布统计
				int accNum = (0 == userInfoLog.getAccountNum()) ? 1 : userInfoLog.getAccountNum();
				int accountNumRange = EnumConstants.getAccNumPerDevRange(accNum);
				
				String[] accNumPerDev = new String[]{
						userInfoLog.getAppID(), 
						userInfoLog.getPlatform(), 
						userInfoLog.getChannel(), 
						userInfoLog.getGameServer(),
						Constants.PLAYER_TYPE_NEWADD, 
						Constants.DIMENSION_DEVICE_ACC_NUM_PER_DEV, 
						accountNumRange+""
				};
				
				mapKeyObj.setSuffix(Constants.SUFFIX_ACCOUNT_NUM_PER_DEV);
				mapKeyObj.setOutFields(accNumPerDev);
				context.write(mapKeyObj, mapValObj);
			}
		}
	}
}
