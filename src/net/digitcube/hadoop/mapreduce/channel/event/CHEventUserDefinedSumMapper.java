package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 以每个独立用户的事件统计作为输入
 * @see CHEventUserDefinedReducer
 * -----------------------------输入----------------------------- 
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
 * 
 * -----------------------------输出----------------------------- 
 * 1.事件统计
 * Key: 			appID,appVersion,channel,country,province,eventID
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_STAT
 * Value: 			count,duration
 * 
 * 2.事件属性统计
 * Key: 			appID,appVersion,channel,country,province,eventID,attr,attrVal
 * Key.Suffix 		Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT
 * Value: 			count,duration
 * 
 */

public class CHEventUserDefinedSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int i = 0;
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.endsWith(Constants.SUFFIX_CHANNEL_EVENT_STAT)) { // 事件统计
			if(array.length < 9){
				return;
			}
			String appID = array[i++];
			String uid = array[i++];
			String eventID = array[i++];
			String appVersion = array[i++];
			String channel = array[i++];
			String country = array[i++];
			String province = array[i++];
			String totalCount = array[i++];
			String totalDuration = array[i++];
			mapKeyObj.setOutFields(new String[] { appID, appVersion, channel, country, province, eventID });
			mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_EVENT_STAT);
			mapValObj.setOutFields(new String[] { "" + totalCount, "" + totalDuration });
			context.write(mapKeyObj, mapValObj);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT)) { // 事件属性统计
			if(array.length < 11){
				return;
			}
			String appID = array[i++];
			String uid = array[i++];
			String eventID = array[i++];
			String attr = array[i++];
			String attrVal = array[i++];
			String appVersion = array[i++];
			String channel = array[i++];
			String country = array[i++];
			String province = array[i++];
			String totalCount = array[i++];
			String totalDuration = array[i++];
			mapKeyObj
					.setOutFields(new String[] { appID, appVersion, channel, country, province, eventID, attr, attrVal });
			mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_EVENT_ATTR_STAT);
			mapValObj.setOutFields(new String[] { "" + totalCount, "" + totalDuration });
			context.write(mapKeyObj, mapValObj);
		}
	}
}
