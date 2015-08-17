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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 资源下载
 * 
 * -----------------------------输入-----------------------------
 * 取自定义事件日志
 * 1.资源下载		_DESelf_Channel_Res_Download
 * 2.资源下载成功	_DESelf_Channel_Download_Result
 * 
 * -----------------------------输出-----------------------------
 * 1.资源下载统计（Type区分资源位和搜索词）
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL
 * Value:			Type,RlID/Keyword
 * 
 * 2.资源位下载统计
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL
 * Value:			One
 * 
 * 3.搜索字下载统计
 * Key:				appID,appVersion,channel,country,province,keyword
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL
 * Value:			One
 * 
 * 4.资源下载成功
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT
 * Value:			One
 * 
 * 
 * @author sam.xie
 * @date 2015年3月4日 下午6:10:50
 * @version 1.0
 */
public class CHResourceDownloadMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

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
		String resId = attrMap.get(AttrKey.RESID);
		String rlId = attrMap.get(AttrKey.RLID);
		String keyword = attrMap.get(AttrKey.KEYWORD);
		
		/** 资源下载 */
		if (Constants.DESELF_CHANNEL_RES_DOWNLOAD.equals(eventLog.getEventId())) {
			if((StringUtils.isBlank(rlId) && StringUtils.isBlank(keyword)) // 资源下载必须满足：有且只有资源位或关键字
					|| (StringUtils.isNotBlank(rlId) && StringUtils.isNotBlank(keyword))){
				return;
			}
			if (StringUtils.isNotBlank(rlId) && StringUtils.isBlank(keyword)) { // 按资源位
				// 1.资源-资源位
				OutFieldsBaseModel outputKey1 = new OutFieldsBaseModel();
				OutFieldsBaseModel outputValue1 = new OutFieldsBaseModel();
				String[] outFields1 = { appID, appVersion, channel, country, province, resId };
				outputKey1.setSuffix(Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL);
				outputKey1.setOutFields(outFields1);
				outputValue1.setOutFields(new String[] { "RL", rlId });
				context.write(outputKey1, outputValue1);

				// 2.资源位
				OutFieldsBaseModel outputKey2 = new OutFieldsBaseModel();
				OutFieldsBaseModel outputValue2 = new OutFieldsBaseModel();
				String[] outFields2 = { appID, appVersion, channel, country, province, rlId };
				outputKey2.setSuffix(Constants.SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL);
				outputKey2.setOutFields(outFields2);
				outputValue2.setOutFields(new String[] { "1" });
				context.write(outputKey2, outputValue2);
			} else if (StringUtils.isBlank(rlId) && StringUtils.isNotBlank(keyword)) { // 按搜索字
				// 1.资源-搜索字
				OutFieldsBaseModel outputKey1 = new OutFieldsBaseModel();
				OutFieldsBaseModel outputValue1 = new OutFieldsBaseModel();
				String[] outFields1 = { appID, appVersion, channel, country, province, resId };
				outputKey1.setSuffix(Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL);
				outputKey1.setOutFields(outFields1);
				outputValue1.setOutFields(new String[] { "KW", keyword });
				context.write(outputKey1, outputValue1);

				// 2.搜索字
				OutFieldsBaseModel outputKey2 = new OutFieldsBaseModel();
				OutFieldsBaseModel outputValue2 = new OutFieldsBaseModel();
				String[] outFields2 = { appID, appVersion, channel, country, province, keyword };
				outputKey2.setSuffix(Constants.SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL);
				outputKey2.setOutFields(outFields2);
				outputValue2.setOutFields(new String[] { "1" });
				context.write(outputKey2, outputValue2);
			}

		} else if (Constants.DESELF_CHANNEL_DOWNLOAD_RESULT.equals(eventLog.getEventId())) {
			/** 资源下载成功 */
			String[] outFields = { appID, appVersion, channel, country, province, resId };
			outputKey.setSuffix(Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT);
			outputKey.setOutFields(outFields);
			outputValue.setOutFields(new String[] { "1" }); //占位而已
			context.write(outputKey, outputValue);
		}
	}

}
