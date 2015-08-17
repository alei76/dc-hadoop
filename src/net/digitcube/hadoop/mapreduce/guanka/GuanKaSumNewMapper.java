package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 统计每关卡的：玩家数、开始次数，成功次数，失败次数，失败退出次数，总时长，成功总时长，失败总时长
 * 失败退出率 = 失败时退出次数/失败次数
 * 失败率 = 失败次数/关卡开始次数
 * 
 * 输入1：
 * appID, platform, channel, gameServer, accountId, isNew(Y/N), isPay(Y/N), levelId,  
 * beginTimes, successTime, failedTimes, failedExitTimes, totalDuration, succDuration, failedDuration 
 * 
 * 输入2：关卡失败原因
 * appID, platform, channel, gameServer, accountId, isNew(Y/N), isPay(Y/N), levelId, failPoint,
 * times
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

public class GuanKaSumNewMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	private String fileName = "";
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
	};
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
			if(fileName.contains(Constants.SUFFIX_GUANKA_FAIL_REASON_DIST)){ // 关卡失败原因分布
				String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
				int i = 0;
				String appId = array[i++];
				String platform = array[i++];
				String channel = array[i++];
				String gameServer = array[i++];
				String accountId = array[i++];
				String isNewPlayer = array[i++];
				String isPayPlayer = array[i++];				
				String guankaId = array[i++];
				String failPoint = array[i++];
				String times = array[i++];
				//活跃玩家
				String[] keyFields = {appId,platform,channel, gameServer, Constants.PLAYER_TYPE_ONLINE, guankaId,failPoint};
				String[] valFields = {times};
				mapKeyObj.setSuffix(Constants.SUFFIX_GUANKA_FAIL_REASON_DIST); // 关卡失败原因分布
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				//新增玩家
				if(Constants.DATA_FLAG_YES.equals(isNewPlayer)){
					keyFields[keyFields.length - 3] = Constants.PLAYER_TYPE_NEWADD;
					mapKeyObj.setOutFields(keyFields);
					mapValObj.setOutFields(valFields);
					context.write(mapKeyObj, mapValObj);
				}
				
				//付费玩家
				if(Constants.DATA_FLAG_YES.equals(isPayPlayer)){
					keyFields[keyFields.length - 3] = Constants.PLAYER_TYPE_PAYMENT;
					mapKeyObj.setOutFields(keyFields);
					mapValObj.setOutFields(valFields);
					context.write(mapKeyObj, mapValObj);
				}
				
			}else{
				String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
				int i = 0;
				String appId = array[i++];
				String platform = array[i++];
				String channel = array[i++];
				String gameServer = array[i++];
				String accountId = array[i++];
				
				String isNewPlayer = array[i++];
				String isPayPlayer = array[i++];
				
				String guankaId = array[i++];
				String beginTimes = array[i++];
				String successTimes = array[i++];
				String failedTimes = array[i++];
				String failedExitTimes = array[i++];
				String totalDuration = array[i++];
				String succDuration = array[i++];
				String failDuration = array[i++];
				
				//统计活跃玩家
				String[] keyFields = new String[]{
						appId,
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
				mapKeyObj.setOutFields(keyFields);
				mapValObj.setOutFields(valFields);
				context.write(mapKeyObj, mapValObj);
				
				//新增玩家
				if(Constants.DATA_FLAG_YES.equals(isNewPlayer)){
					keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_NEWADD;
					mapKeyObj.setOutFields(keyFields);
					mapValObj.setOutFields(valFields);
					context.write(mapKeyObj, mapValObj);
				}
				
				//付费玩家
				if(Constants.DATA_FLAG_YES.equals(isPayPlayer)){
					keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_PAYMENT;
					mapKeyObj.setOutFields(keyFields);
					mapValObj.setOutFields(valFields);
					context.write(mapKeyObj, mapValObj);
				}
			}
		}catch(Exception e){
			System.out.println("----GuankasumNewError，do nothing----");
		}
	}
}
