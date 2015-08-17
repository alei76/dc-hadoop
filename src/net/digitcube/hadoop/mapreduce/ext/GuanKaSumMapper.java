package net.digitcube.hadoop.mapreduce.ext;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.PlayerType;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 主要逻辑
 * 统计每关卡的：玩家数、开始次数，成功次数，失败次数，失败退出次数，总时长，成功总时长，失败总时长
 * 失败退出率 = 失败时退出次数/失败次数
 * 失败率 = 失败次数/关卡开始次数
 * 
 * 输入：
 * appID, platform, channel, gameServer, accountId, uid, isNew(Y/N), isPay(Y/N), levelId,  
 * beginTimes, successTime, failedTimes, failedExitTimes, totalDuration, succDuration, failedDuration 
 * 
 * 输出：
 * appId,
 * platform,
 * channel,
 * gameServer,
 * playerType,
 * guankaId, //vkey1
 * dimenType, //类型(人数、时长、付费...)
 * vkey2, //(成功次数、失败次数、成功时长、失败时长...)
 * value
 */

public class GuanKaSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = array[i++];
		String appVer = array[i++];
		String platform = array[i++];
		String channel = array[i++];
		String gameServer = array[i++];
		String accountId = array[i++];
		String uid = array[i++];
		String warpedPlayerType = array[i++]; //玩家类型：新增/活跃/曾经付费
		
		String guankaId = array[i++];
		String beginTimes = array[i++];
		String successTimes = array[i++];
		String failedTimes = array[i++];
		String failedExitTimes = array[i++];
		String totalDuration = array[i++];
		String succDuration = array[i++];
		String failDuration = array[i++];
		
		PlayerType player = new PlayerType(StringUtil.convertInt(warpedPlayerType, 0));
		
		//统计活跃玩家
		String[] keyFields = new String[]{
				appId,
				appVer,
				platform,
				channel,
				gameServer,
				Constants.PLAYER_TYPE_ONLINE,
				guankaId
		};
		String[] valFields = new String[]{
				beginTimes,
				successTimes,
				failedTimes,
				failedExitTimes,
				totalDuration,
				succDuration,
				failDuration
		};
		if(player.isOnline()){
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
		}
		
		//新增玩家
		if(player.isNewAdd()){
			keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_NEWADD;
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
		}
		
		//付费玩家
		if(player.isEverPay()){
			keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_PAYMENT;
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
