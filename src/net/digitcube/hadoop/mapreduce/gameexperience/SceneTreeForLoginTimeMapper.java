package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 对同一玩家同一次登录所浏览的场景按浏览时间排序并输出
 * key:appID|platform|channel|gameServer|Uid|loginTime
 * value:sceneStartTime|sceneName|sceneEndTime-sceneStartTime
 * 后缀 APP_SCENETREE_4_LOGINTIME
 * @author mikefeng
 *
 */
public class SceneTreeForLoginTimeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = new EventLog(arr);
		String appID = eventLog.getAppID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		String UID = eventLog.getUID();
		Map<String, String> attrMap = eventLog.getArrtMap();	
		String sceneName = attrMap.get("scene_name");
		String sceneStartTime = attrMap.get("scene_start_time");
		String sceneEndTime = attrMap.get("scene_end_time");
		String loginTime = attrMap.get("login_time");
		long sceneStartTimeL = StringUtil.convertLong(sceneStartTime, 0);
		long sceneEndTimeL = StringUtil.convertLong(sceneEndTime, 0);
		long duration = sceneEndTimeL - sceneStartTimeL;
		
		if( StringUtil.isEmpty(sceneName) || StringUtil.isEmpty(sceneStartTime) 
				|| StringUtil.isEmpty(sceneEndTime)	|| StringUtil.isEmpty(loginTime)) {
			return;
		}
		
		String[] keyFields = new String[]{
				appID,
				platform,
				channel,
				gameServer,
				UID,
				loginTime
		};
		String[] valFields = new String[]{
				sceneStartTime,
				sceneName,
				duration + ""
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
}
