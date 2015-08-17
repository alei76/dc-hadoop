package net.digitcube.hadoop.mapreduce.html5.event;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
 * 		APPID,Platform,PromotePlatForm,Domain,Referer,EventID
 * value：
 * 		UID,Duration
 * 
 * Reduce:
 * 		APPID,Platform,PromotePlatForm,Domain,Referer,EventID,
 * 		count(UID)(事件发生次数)
 * 		count(distinct(UID))(事件发生独立用户数)
 * 		sum(Duration)(事件时长总和)
 * 
 * b) 事件属性级统计输出
 * Map:
 * key：
 * 		APPID,Platform,PromotePlatForm,Domain,Referer,EventID,EventAttr
 * value：
 * 		UID
 * 
 * Reduce:
 * 		APPID,Platform,PromotePlatForm,Domain,Referer,EventID,EventAttrLabel,EventAttrValue
 * 		count(UID)(事件属性发生次数)
 * 		count(distinct(UID))(事件属性发生独立用户数)
 */

public class H5EventStatisticsMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

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
		
		if(eventLog.isReferDomainEmpty() || StringUtil.isEmpty(eventLog.getUID())){
			return;
		}
		
		String appID = eventLog.getAppID();
		String uID = eventLog.getUID();
		String platform = eventLog.getPlatform();
		String promotePlatForm = eventLog.getH5PromotionApp();
		String domain = eventLog.getH5Domain();
		String refer = eventLog.getH5Refer();
		
		String eventID = eventLog.getEventId();
		String duration = ""+eventLog.getDuration();
		String eventAttr = eventLog.getEventAttr();
		Map<String, String> attrMap = eventLog.getArrtMap();		

		
		//事件统计
		
		/*
		 * 如果事件属性中包含事件汇总标记 _DESelf_Count
		 * 说明事件发生的次数是汇总过的
		 * 在 '事件属性统计' 中已事件统计的方式进行输出即可
		 * 
		 * 如果不包含  _DESelf_Count ,则把当前事件次数即为 1
		 */
		if(!eventAttr.contains(Constants.EVENT_ATTR_ID_COUNT)){
			String[] eventKey = new String[]{appID,platform,promotePlatForm,domain,refer,eventID};
			
			String[] eventVal = new String[]{"1", uID, duration};
			mapKeyObj.setSuffix(Constants.SUFFIX_H5_EVENT_STAT);
			mapKeyObj.setOutFields(eventKey);
			mapValObj.setOutFields(eventVal);
			context.write(mapKeyObj, mapValObj);
		}
		
		
		//事件属性统计
		Set<Entry<String, String>> set = attrMap.entrySet();
		for(Entry<String, String> entry : set){
				String attrLabel = entry.getKey();
				String attrValue = entry.getValue();
				
				/*
				 * 如果事件属性中包含事件汇总标记 _DESelf_Count
				 * 说明事件发生的次数是汇总过的,输出事件统计
				 */
				if(Constants.EVENT_ATTR_ID_COUNT.equals(attrLabel)){
					//特殊处理：只为统计事件发生的次数,与别的事件属性不同
					String eventCount = attrValue;
					String[] eventKeyArr = new String[]{appID,platform,promotePlatForm,domain,refer,eventID};			
					
					String[] eventValArr = new String[]{eventCount, uID, duration};
					
					//设置事件统计后缀
					mapKeyObj.setSuffix(Constants.SUFFIX_H5_EVENT_STAT);
					mapKeyObj.setOutFields(eventKeyArr);
					mapValObj.setOutFields(eventValArr);
					context.write(mapKeyObj, mapValObj);		
				}else{
					String[] eventAttrKey = new String[]{appID,platform,promotePlatForm,domain,refer,eventID,attrLabel,attrValue};
					
					String[] eventAttrVal = new String[]{uID};
					mapKeyObj.setSuffix(Constants.SUFFIX_H5_EVENT_ATTR_STAT);
					mapKeyObj.setOutFields(eventAttrKey);
					mapValObj.setOutFields(eventAttrVal);
					context.write(mapKeyObj, mapValObj);				
				}
		}
	}
}
