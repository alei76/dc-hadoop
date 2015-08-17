package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * -----------------------------输入----------------------------- 
 * @see CHEventUserDefinedSumMapper
 * 
 * -----------------------------输出-----------------------------
 * 按版本,渠道,国家,地区统计事件消息数，独立用户数，时长
 * 1.事件统计 
 * Key: 			appID,appVersion,channel,country,province,eventID
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_STAT
 * Value: 			count,uniqCount,duration
 * 
 * 2.事件属性统计
 * Key: 			appID,appVersion,channel,country,province,eventID,attr,attrVal
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT
 * Value:			count,uniqCount,duration,
 * 
 */

public class CHEventUserDefinedSumReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int totalDuration = 0; // 事件/属性总时长
		int totalCount = 0; // 消息数
		int totalUniqCount = 0;// 独立用户数
		for (OutFieldsBaseModel val : values) {
			int count = StringUtil.convertInt(val.getOutFields()[0], 0);
			int duration = StringUtil.convertInt(val.getOutFields()[1], 0);
			totalCount += count;
			totalDuration += duration;
			totalUniqCount++;
		}
		String[] outFields = new String[] { "" + totalCount, "" + totalUniqCount, "" + totalDuration };
		valObj.setOutFields(outFields);
		context.write(key, valObj);
	}
}
