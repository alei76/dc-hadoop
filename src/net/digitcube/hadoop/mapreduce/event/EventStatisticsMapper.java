package net.digitcube.hadoop.mapreduce.event;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.IOSChannelUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 以自定义事件 24 个时间片的原始日志作为输入
 * a) 在事件级别对每个事件的事件次数、独立用户数（设备）以及时间时长进行统计
 * b) 在事件属性级别对每个事件的事件次数、独立用户数（设备）进行统计
 * 
 * 输入：
 * 自定义事件 24 个时间片的原始日志
 * 
 * 
 * a) 事件级统计输出
 * Map：
 * key：
 * 		APPID,Platform,Channel,GameServer,EventID
 * value：
 * 		UID,Duration
 * 
 * Reduce:
 * 		APPID,Platform,Channel,GameServer,EventID,
 * 		count(UID)(事件发生次数)
 * 		count(distinct(UID))(事件发生独立用户数)
 * 		sum(Duration)(事件时长总和)
 * 
 * b) 事件属性级统计输出
 * Map:
 * key：
 * 		APPID,Platform,Channel,GameServer,EventID,EventAttr
 * value：
 * 		UID
 * 
 * Reduce:
 * 		APPID,Platform,Channel,GameServer,EventID,EventAttrLabel,EventAttrValue
 * 		count(UID)(事件属性发生次数)
 * 		count(distinct(UID))(事件属性发生独立用户数)
 */

public class EventStatisticsMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(array);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
			return;
		}
		
		//20141217 : 自定义事件有时会上报异常值
		//如果事件时长大于 7*24*3600，则视为异常记录丢弃
		if(eventLog.getDuration() > 7*24*3600){
			return;
		}
		
		String appID = eventLog.getAppID();
		String uID = eventLog.getUID();
		String platform = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();
		
		String eventID = eventLog.getEventId();
		String duration = ""+eventLog.getDuration();
		String eventAttr = eventLog.getEventAttr();
		Map<String, String> attrMap = eventLog.getArrtMap();
		
		//Added at 20140606 : iOS 渠道修正
		//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		//if(MRConstants.PLATFORM_iOS_STR.equals(platform)){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(appID, 
																	 uID, 
																	 platform, 
																	 channel);
			channel = reviseChannel;
		//}
		
		// Added at 20140811: 登录前上报的特定事件
		// 为了兼容登录前上报的事件，必须用特定的事件 ID 上报事件
		// 真正的事件 ID 放在事件属性 map 里
		if(Constants.DESelf_BeforeLogin_Event.equals(eventID) 
				&& null != attrMap.get(Constants.DESelf_BeforeLogin_Event)){
				eventID = attrMap.get(Constants.DESelf_BeforeLogin_Event);
				//该事件属性是我们的 SDK 添加的，不必统计属性详情，删除掉
				attrMap.remove(Constants.DESelf_BeforeLogin_Event);
		}
		
		//事件统计
		
		/*
		 * 如果事件属性中包含事件汇总标记 _DESelf_Count
		 * 说明事件发生的次数是汇总过的
		 * 在 '事件属性统计' 中已事件统计的方式进行输出即可
		 * 
		 * 如果不包含  _DESelf_Count ,则把当前事件次数即为 1
		 */
		if(!eventAttr.contains(Constants.EVENT_ATTR_ID_COUNT)){
			//真实区服
			String[] eventKey = new String[]{appID,platform,channel,gameServer,eventID};
			//全服
			String[] eventKey_AllGS = new String[]{appID,platform,channel,MRConstants.ALL_GAMESERVER,eventID};
			
			String[] eventVal = new String[]{"1", uID, duration};
			mapKeyObj.setSuffix(Constants.SUFFIX_EVENT_STAT);
			mapKeyObj.setOutFields(eventKey);
			mapValObj.setOutFields(eventVal);
			context.write(mapKeyObj, mapValObj);
			
			//全服
			mapKeyObj.setOutFields(eventKey_AllGS);
			context.write(mapKeyObj, mapValObj);
		}
		
		
		//事件属性统计
		//String[] attrArr = eventAttr.split(",");
		//for(String attr : attrArr){
		Set<Entry<String, String>> set = attrMap.entrySet();
		for(Entry<String, String> entry : set){
			//String[] vals = attr.split(":");
			//if(vals.length > 1){
				//String attrLabel = vals[0];
				//String attrValue = vals[1];
				String attrLabel = entry.getKey();
				String attrValue = entry.getValue();
				
				// startTime,endTime 为内部标识用，无需统计
				if("startTime".equals(attrLabel) || "endTime".equals(attrLabel)){
					continue;
				}
				
				/*
				 * 如果事件属性中包含事件汇总标记 _DESelf_Count
				 * 说明事件发生的次数是汇总过的,输出事件统计
				 */
				if(Constants.EVENT_ATTR_ID_COUNT.equals(attrLabel)){
					//特殊处理：只为统计事件发生的次数,与别的事件属性不同
					String eventCount = attrValue;
					//真实区服
					String[] eventKeyArr = new String[]{appID,platform,channel,gameServer,eventID};
					//全服
					String[] eventKeyArr_AllGS = new String[]{appID,platform,channel,MRConstants.ALL_GAMESERVER,eventID};
					
					String[] eventValArr = new String[]{eventCount, uID, duration};
					
					//设置事件统计后缀
					mapKeyObj.setSuffix(Constants.SUFFIX_EVENT_STAT);
					mapKeyObj.setOutFields(eventKeyArr);
					mapValObj.setOutFields(eventValArr);
					context.write(mapKeyObj, mapValObj);
					//全服
					mapKeyObj.setOutFields(eventKeyArr_AllGS);
					context.write(mapKeyObj, mapValObj);
				}else{
					//真实区服
					String[] eventAttrKey = new String[]{appID,platform,channel,gameServer,eventID,attrLabel,attrValue};
					//全服
					String[] eventAttrKey_AllGS = new String[]{appID,platform,channel,MRConstants.ALL_GAMESERVER,eventID,attrLabel,attrValue};
					
					String[] eventAttrVal = new String[]{uID};
					mapKeyObj.setSuffix(Constants.SUFFIX_EVENT_ATTR_STAT);
					mapKeyObj.setOutFields(eventAttrKey);
					mapValObj.setOutFields(eventAttrVal);
					context.write(mapKeyObj, mapValObj);
					//全服
					mapKeyObj.setOutFields(eventAttrKey_AllGS);
					context.write(mapKeyObj, mapValObj);
				}
			//}
		}
	}
}
