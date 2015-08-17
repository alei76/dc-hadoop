package net.digitcube.hadoop.mapreduce.ext;

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
 * 输出：
 * appId, maxVer, platform, channel, gameServer, accountId, uid, playerType(新增/活跃/曾经付费)
 * guankaId, beginTimes, successTimes, failedTimes, failedExitTimes, duration, succDuration, failDuration
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
		String uid = eventLog.getUID();
		int duration = eventLog.getDuration();
		Map<String, String> map = eventLog.getArrtMap();
		String levelId = map.get("levelId");
		//20141204：关卡统计中去掉 seqno
		//String seqno = map.get("seqno");
		
		if(StringUtil.isEmpty(levelId)){//无效关卡 id
			return;
		}
		
		String[] appIdAndVer = StringUtil.getAppIdAndVer(appID);
		String pureAppId = appIdAndVer[0];
		String appVersion = appIdAndVer[1];
		//真实区服
		String[] keyFields = new String[]{
				pureAppId, //pure appId without version
				platform,
				gameServer,
				accountId
		};
		String[] valFields = null;
		
		if(fileName.contains(Constants.DESelf_LevelsBegin)){
			valFields = new String[]{
					appVersion,
					uid,
					channel,
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
						appVersion,
						uid,
						channel,
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
		//mapValObj.setSuffix(uid);
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
		
		//全服
		keyFields[2] = MRConstants.ALL_GAMESERVER;
		context.write(mapKeyObj, mapValObj);
		
	}
}
