package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 资源搜索
 * 
 * -----------------------------输入-----------------------------
 * 取自定义事件日志
 * 1.资源搜索_DESelf_Channel_Res_Search
 * 
 * -----------------------------输出-----------------------------
 * 1.资源搜索关键字统计
 * Key:				appID,appVersion,channel,country,province,keyword
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_SEARCH
 * Value:			one
 * 
 * @author sam.xie
 * @date 2015年3月4日 下午2:39:50
 * @version 1.0
 */
public class CHResourceSearchMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog2(arr);
		} catch (Exception e) {
			return;
		}
		String appID = eventLog.getAppID();
		String appVersion = eventLog.getAppVersion();
		String channel = eventLog.getChannel();
		String country = eventLog.getExtendValue(ExtendKey.CNTY);
		String province = eventLog.getExtendValue(ExtendKey.PROV);

		Map<String, String> attrMap = eventLog.getAttrMap();

		if (Constants.DESELF_CHANNEL_RES_SEARCH.equals(eventLog.getEventId())
				&& StringUtils.isNotBlank(attrMap.get(AttrKey.KEYWORD))) {
			String[] outFields = new String[] { appID, appVersion, channel, country, province,
					attrMap.get(AttrKey.KEYWORD) };
			outputKey.setOutFields(outFields);
			outputKey.setSuffix(Constants.SUFFIX_CHANNEL_RES_SEARCH);
			context.write(outputKey, one);
		}
	}

}
