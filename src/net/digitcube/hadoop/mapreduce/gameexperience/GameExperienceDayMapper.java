package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.gameexperience.po.Point;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入后缀:DESelf_GE_SceneInfo
 * 1 计算场景总时长 、上行、下行、次数
 * key:appID|platform|channel|gameServer|sceneName|resolution|brand
 * value:[(sceneEndTime-sceneStartTime)/1000] |networkTxFlow| networkRxFlow
 * 
 * 2 场景手势统计
 * key:appID|platform|channel|gameServer|sceneName|resolution|brand|type|gestureType
 * value:1
 * 
 * 3 热力图
 * key:appID|platform|channel|gameServer|sceneName|resolution|brand|type|x|y
 * value:1
 * 
 * 4 性能分析  fps cpu ram
 * INT I = 0   
 * key:appID|platform|channel|gameServer|sceneName|resolution|brand|type|I
 * value:X
 * 
 * @author mikefeng
 *
 */
public class GameExperienceDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(array);
		}catch(Exception e){
			return;
		}
		
		String appID = eventLog.getAppID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		String resolution = eventLog.getResolution();
		String brand = eventLog.getBrand();
		
		Map<String, String> attrMap = eventLog.getArrtMap();	
		String sceneName = attrMap.get("scene_name");
		String sceneStartTime = attrMap.get("scene_start_time");
		String sceneEndTime = attrMap.get("scene_end_time");
		String networkTxFlow = attrMap.get("network_tx_flow");
		String networkRxFlow = attrMap.get("network_rx_flow");
		String gestureListType = attrMap.get("gesture_list");
		String touchpointList = attrMap.get("touchpoint_list");
		String fpsList = attrMap.get("fps_list");
		String cpuList = attrMap.get("cpu_list");
		String ramList = attrMap.get("memory_list");		
		String orientation = attrMap.get("orientation");
		long sceneStartTimeLong = StringUtil.convertLong(sceneStartTime, 0);
		long sceneEndTimeLong = StringUtil.convertLong(sceneEndTime, 0);
		long duration = sceneEndTimeLong-sceneStartTimeLong;
		
		if( StringUtil.isEmpty(sceneName)					
				|| null == gestureListType || null == touchpointList
				|| null == fpsList || null == cpuList || null == ramList) {
			return;
		}	
		
		if(StringUtil.isEmpty(resolution) 
				|| StringUtil.split(resolution, MRConstants.SEPERATOR_SCENE_RESOLUTION).length != 2){
			return;
		}
		
		if(StringUtil.isEmpty(orientation) 
				||( !(Constants.SCENEINFO_ORIENTATION_PORTRAIT.equals(orientation))
						&&!(Constants.SCENEINFO_ORIENTATION_LANDSCAPE.equals(orientation)) )
						){
			return;
		}		
		
		if(StringUtil.isEmpty(networkTxFlow)){
			networkTxFlow = "0";
		}
		
		if(StringUtil.isEmpty(networkRxFlow)){
			networkRxFlow = "0";
		}
		
		// 真实区服
		String[] keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand };
		String[] valFields = new String[] { String.valueOf(duration), networkTxFlow, networkRxFlow ,orientation};
		// 全服
		String[] keyFields2 = new String[] { appID, platform,channel, MRConstants.ALL_GAMESERVER, sceneName,resolution,brand };
		//场景总时长 、上行、下行		
		statDurationRxRt(keyFields,valFields,keyFields2,valFields,context);
		
		
		//场景手势
		for(String gestureType : StringUtil.split(gestureListType, MRConstants.SEPERATOR_SCENE_ARRAY)){
			// 真实区服
			keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand,Constants.SCENEINFO_GESTURE,gestureType };
			// 全服
			keyFields2 = new String[] { appID, platform,channel, MRConstants.ALL_GAMESERVER, sceneName,resolution,brand,Constants.SCENEINFO_GESTURE,gestureType };
			valFields = new String[] {"1",orientation};
			statGesture(keyFields,valFields,keyFields2,valFields,context);
		}
		
		//热力图
		for(String touchpoint : StringUtil.split(touchpointList,MRConstants.SEPERATOR_SCENE_ARRAY)){
			if(StringUtil.split(touchpoint,MRConstants.SEPERATOR_SCENE_POINT).length==2 
					&& StringUtil.split(resolution,MRConstants.SEPERATOR_SCENE_RESOLUTION).length==2 ){			
				Point point = transXy(touchpoint,orientation,resolution);
				// 真实区服
				keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand,Constants.SCENEINFO_TOUCH,point.getX(),point.getY() };
				// 全服
				keyFields2 = new String[] { appID, platform,channel,  MRConstants.ALL_GAMESERVER, sceneName,resolution,brand,Constants.SCENEINFO_TOUCH,point.getX(),point.getY() };
				valFields = new String[] {"1",orientation};
				statTouchpoint(keyFields,valFields,keyFields2,valFields,context);
			}
		}
		
		//性能分析
		//fps
		int i = 0;
		for(String fps : StringUtil.split(fpsList,MRConstants.SEPERATOR_SCENE_ARRAY)){
			// 真实区服
			keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand,Constants.SCENEINFO_PA_FPS,String.valueOf(i) };
			valFields = new String[] {fps,orientation};
			// 全服
			keyFields2 = new String[] { appID, platform,channel,  MRConstants.ALL_GAMESERVER, sceneName,resolution,brand,Constants.SCENEINFO_PA_FPS,String.valueOf(i) };
			statPaFps(keyFields,valFields,keyFields2,valFields,context);
			i++;
		}
		
		//cpu
		i = 0;
		for(String cpu : StringUtil.split(cpuList,MRConstants.SEPERATOR_SCENE_ARRAY)){
			// 真实区服
			keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand,Constants.SCENEINFO_PA_CPU,String.valueOf(i) };
			valFields = new String[] {cpu,orientation};
			// 全服
			keyFields2 = new String[] { appID, platform,channel,  MRConstants.ALL_GAMESERVER, sceneName,resolution,brand,Constants.SCENEINFO_PA_CPU,String.valueOf(i) };
			statPaCpu(keyFields,valFields,keyFields2,valFields,context);
			i++;
		}
		
		//ram
		i = 0;
		for(String ram : StringUtil.split(ramList,MRConstants.SEPERATOR_SCENE_ARRAY)){
			// 真实区服
			keyFields = new String[] { appID, platform,channel, gameServer, sceneName,resolution,brand,Constants.SCENEINFO_PA_RAM,String.valueOf(i) };
			valFields = new String[] {ram,orientation};
			// 全服
			keyFields2 = new String[] { appID, platform,channel,  MRConstants.ALL_GAMESERVER, sceneName,resolution,brand,Constants.SCENEINFO_PA_RAM,String.valueOf(i) };
			statPaRam(keyFields,valFields,keyFields2,valFields,context);
			i++;
		}
		
	}	

	//场景总时长 、上行、下行
	private void statDurationRxRt(String[] keyFields,String[] valFields,
				String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_DURATIONRXRT);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);
	}
	
	//场景手势
	private void statGesture(String[] keyFields,String[] valFields,
			String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_GESTURE);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);
	}
	
	// 转换xy
	private Point transXy(String touchpoint,String orientation,String resolution){
		String[] xy = touchpoint.split(MRConstants.SEPERATOR_SCENE_POINT);
		Point point = new Point(xy,orientation);		
		return point.transPoint(orientation,resolution);		
	}
	
	//热力图
	private void statTouchpoint(String[] keyFields,String[] valFields,
				String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_TOUCH);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);
	}
	
	//性能分析 fps
	private void statPaFps(String[] keyFields,String[] valFields,
			String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_PA_FPS);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);		
	}
	
	//性能分析 cpu
	private void statPaCpu(String[] keyFields,String[] valFields,
			String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_PA_CPU);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);		
	}
	
	//性能分析 ram
	private void statPaRam(String[] keyFields,String[] valFields,
			String[] keyFields2,String[] valFields2,Context context)throws IOException, InterruptedException{
		keyObj.setSuffix(Constants.SUFFIX_SCENEINFO_PA_RAM);
		// 真实区服
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
		// 全服
		keyObj.setOutFields(keyFields2);
		valObj.setOutFields(valFields2);
		context.write(keyObj, valObj);		
	}
}
