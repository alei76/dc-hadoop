package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 主要逻辑 以用户自定义事件 24 个时间片的原始日志作为输入
 * a) 在事件级别对每个事件的事件次数、独立用户数（设备）以及时间时长进行统计
 * b) 在事件属性级别对每个事件的事件次数、独立用户数（设备）进行统计
 * 
 * -----------------------------输入----------------------------- 
 * 自定义事件 24 个时间片的原始日志
 * 
 * -----------------------------输出----------------------------- 
 * 每个独立用户的事件和事件属性统计 
 * 1.事件统计
 * Key: 			appID,uid,eventID
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_STAT
 * Value: 			appVersion,channel,country,province,startTime,duration
 * 
 * 2.事件属性统计
 * Key: 			appID,uid,eventID,attr,attrVal
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT
 * Value:			appVersion,channel,country,province,startTime,duration
 * 
 */

public class CHEventUserDefinedMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog2(array);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}
		int duration = StringUtil.convertInt(eventLog.getDuration(), 0);
		// 如果事件时长大于 7*24*3600，则视为异常记录丢弃
		if (duration > 7 * 24 * 3600) {
			return;
		}
		String startTime = eventLog.getStartTime();
		String appID = eventLog.getAppID();
		String appVersion = eventLog.getAppVersion();
		String channel = eventLog.getChannel();
		String country = eventLog.getExtendValue(ExtendKey.CNTY);
		String province = eventLog.getExtendValue(ExtendKey.PROV);
		String UID = eventLog.getUID();

		String eventID = eventLog.getEventId();
		Map<String, String> attrMap = eventLog.getAttrMap();

		// 事件统计
		mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_EVENT_STAT);
		mapKeyObj.setOutFields(new String[] { appID, UID, eventID });
		mapValObj.setOutFields(new String[] { appVersion, channel, country, province, startTime, "" + duration });
		context.write(mapKeyObj, mapValObj);

		// 事件属性
		for (Entry<String, String> entry : attrMap.entrySet()) {
			String attr = entry.getKey();
			String attrVal = entry.getValue();
			mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT);
			mapKeyObj.setOutFields(new String[] { appID, UID, eventID, attr, attrVal });
			mapValObj.setOutFields(new String[] { appVersion, channel, country, province, startTime, "" + duration });
			context.write(mapKeyObj, mapValObj);
		}
	}
}
