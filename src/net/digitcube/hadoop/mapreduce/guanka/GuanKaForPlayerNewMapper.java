package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 对玩家当天的每个关卡输出一条记录，统计信息包括：
 * 开始次数，成功次数，失败次数，失败退出次数，总时长，成功总时长，失败总时长
 * 
 * 输出1：
 * appID, platform, channel, gameServer, accountId, isNew(Y/N), isPay(Y/N), levelId, 
 * beginTimes, successTime, failedTimes, failedExitTimes, totalDuration, succDuration, failedDuration
 * 
 * 输出2：关卡失败原因
 * appID, platform, channel, gameServer, accountId, isNew(Y/N), isPay(Y/N), levelId, failPoint,
 * times
 */

public class GuanKaForPlayerNewMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		EventLog eventLog = new EventLog(array);
		String appID = eventLog.getAppID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		String accountId = eventLog.getAccountID();
		int duration = eventLog.getDuration();
		Map<String, String> map = eventLog.getArrtMap();
		String levelId = map.get("levelId");
		//20141204：关卡统计中去掉 seqno
		//String seqno = map.get("seqno");
		
		if(StringUtil.isEmpty(levelId)){//无效关卡 id
			return;
		}
		
		//真实区服
		String[] keyFields = new String[]{
				appID,
				platform,
				channel,
				gameServer,
				accountId
		};
		String[] valFields = null;
		
		if(fileName.contains(Constants.DESelf_LevelsBegin)){
			valFields = new String[]{
					Constants.DATA_FLAG_GUANKA_BEGIN,
					levelId
			};
		}else if(fileName.contains(Constants.DESelf_LevelsEnd)){
				String result = map.get("result");
				String endTime = map.get("endTime");
				String loginTime = map.get("loginTime");
				String failPoint = map.get("failPoint");
				
				//无效数据
				if(StringUtil.isEmpty(result) || StringUtil.isEmpty(endTime) || StringUtil.isEmpty(loginTime)){
					return;
				}
				
				valFields = new String[]{
						Constants.DATA_FLAG_GUANKA_END,
						levelId,
						result,
						endTime,
						loginTime,
						duration+""
				};
				
				// 失败原因分布
				if(Constants.DATA_FLAG_EVENT_FALSE.equals(result)){
					failPoint = StringUtil.isEmpty(failPoint)?"-":failPoint;
					OutFieldsBaseModel mapKey = new OutFieldsBaseModel();
					OutFieldsBaseModel mapVal = new OutFieldsBaseModel();
					mapKey.setSuffix(Constants.SUFFIX_GUANKA_FAIL_REASON_DIST);
					mapKey.setOutFields(new String[]{appID,platform,channel,gameServer,accountId,levelId,failPoint}); // 加入failPoint失败原因
					mapVal.setOutFields(new String[]{"FR"});
					context.write(mapKey, mapVal);
					
					// 全服
					mapKey.setOutFields(new String[]{appID,platform,channel,MRConstants.ALL_GAMESERVER,accountId,levelId,failPoint}); // 加入failPoint失败原因
					context.write(mapKey, mapVal);
				}
		}
		
		if(null == valFields){
			return;
		}
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
		
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
		
	}
}
