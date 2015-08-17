package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * -----------------------------输入----------------------------- 
 * @see CHEventUserDefinedMapper
 * 
 * -----------------------------输出----------------------------- 
 * 每个独立用户的事件和事件属性统计 
 * 1.事件统计
 * Key: 			appID,uid,eventID
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_STAT
 * Value: 			appVersion,channel,country,province,totalCount,totalDuration
 * 
 * 2.事件属性统计
 * Key: 			appID,uid,eventID,attr,attrVal
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT
 * Value: 			appVersion,channel,country,province,totalCount,totalDuration
 */

public class CHEventUserDefinedReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] tmpValue = null;
		int tmpTime = 0;
		int totalDuration = 0; // 事件/属性总时长
		int totalCount = 0; // 消息数
		for (OutFieldsBaseModel val : values) {
			int i = 0;
			String appVersion = val.getOutFields()[i++];
			String channel = val.getOutFields()[i++];
			String country = val.getOutFields()[i++];
			String province = val.getOutFields()[i++];
			int time = StringUtil.convertInt(val.getOutFields()[i++], 0);
			int duration = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if (tmpValue == null || tmpTime > time) { // 取时间最早事件中的版本，渠道，国家，地区
				tmpValue = new String[] { appVersion, channel, country, province };
			}
			totalDuration += duration;
			totalCount++;
		}

		String[] outFields = new String[] { tmpValue[0], tmpValue[1], tmpValue[2], tmpValue[3], "" + totalCount,
				"" + totalDuration };
		valObj.setOutFields(outFields);
		context.write(key, valObj);
	}
}
