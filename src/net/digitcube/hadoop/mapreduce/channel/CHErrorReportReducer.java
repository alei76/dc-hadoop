package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * App使用习惯
 * 应用即资源，为了避免与渠道本身的App混淆，这里用Res表示
 * 
 * 
 * -----------------------------输入-----------------------------
 * @see CHErrorReportMapper
 * 
 * -----------------------------输出-----------------------------
 * 1.错误详情（错误发生次数，最后一次错误时间）
 * Key:				appID,appVersion,channel,country,province,errorMD5
 * Value:			errorTitle,errorContent,errorCount,lastErrorTime
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_ERROR_REPORT_DETAIL
 * 
 * 2.错误分布（操作系统和机型统计）
 * Key:				appID,appVersion,channel,country,province,errorMD5,os,brand
 * Value:			errorTitle,errorContent,errorCount,lastErrorTime
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_ERROR_REPORT_DIST
 * 
 * 
 * @author sam.xie
 * @date 2015年3月19日 上午10:56:50
 * @version 1.0
 */
public class CHErrorReportReducer extends
		Reducer<OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, BigFieldsBaseModel> {

	private BigFieldsBaseModel mapValueObj = new BigFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<BigFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int errorCount = 0;
		int lastErrorTime = 0;
		String errorTitle = "-";
		String errorContent = "";

		for (BigFieldsBaseModel value : values) {
			String[] outFields = value.getOutFields();
			int time = StringUtil.convertInt(outFields[0], 0);
			String title = outFields[1];
			String content = outFields[2];
			lastErrorTime = lastErrorTime < time ? time : lastErrorTime;
			errorTitle = title;
			errorContent = content;
			errorCount++;
		}
		mapValueObj.setOutFields(new String[] { errorTitle, errorContent, "" + errorCount, "" + lastErrorTime });
		context.write(key, mapValueObj);
	}
}
