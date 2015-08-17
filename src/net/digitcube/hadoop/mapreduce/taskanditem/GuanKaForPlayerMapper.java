package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 对某一玩家当天的所有关卡的开始次数，成功次数，失败次数，失败退出次数，完成关卡的总时长记录进行汇总
 * 
 * 输出：
 * appID, platform, channel, gameServer, accountId, levelId,  
 * dataFlag[BT(beginTimes),ET(endTimes)...], value[beginTimes, successTime, failedTimes, failedExitTimes, totalDuration]
 */

public class GuanKaForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
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
		String seqno = map.get("seqno");
		//levelId + seqno 需一起作为 reduce 端 map 中的 key
		levelId = levelId + "|" + seqno; 
		
		//无效数据
		if(null == levelId || null == seqno){
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
				if(null == result || null == endTime || null == loginTime){
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
