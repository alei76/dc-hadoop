package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.net.URLDecoder;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MD5Util;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 错误日志
 * 
 * -----------------------------输入-----------------------------
 * 取自定义事件日志
 * 1.应用启动在线时长_DESelf_Error_Report
 * 
 * -----------------------------输出----------------------------- 
 * 1.错误详情
 * Key:				appID,appVersion,channel,country,province,errorMD5
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_ERROR_REPORT_DETAIL
 * Value:			errorTime,errorTitle,errorContent
 * 
 * 2.错误分布
 * Key:				appID,appVersion,channel,country,province,errorMD5,os,brand 
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_ERROR_REPORT_DIST
 * Value:			errorTime,errorTitle,errorContent
 * 
 * @author sam.xie
 * @date 2015年3月19日 上午9:56:50
 * @version 1.0
 * 
 */
public class CHErrorReportMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private BigFieldsBaseModel mapValueObj = new BigFieldsBaseModel();
	private final static int UTF_MAX_LENGTH = 65535;

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
		String os = eventLog.getExtendValue(ExtendKey.OS);
		String brand = eventLog.getExtendValue(ExtendKey.BRAND);
		String errorTime = eventLog.getAttrMap().get(AttrKey.ERROR_TIME);
		String errorTitle = eventLog.getAttrMap().get(AttrKey.TITLE);
		String errorContent = eventLog.getAttrMap().get(AttrKey.CONTENT);
		if (StringUtil.isEmpty(errorContent) || StringUtil.isEmpty(errorTitle)
				|| MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(errorContent)
				|| MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(errorTitle)) {
			return;
		}
		String errorMD5 = MD5Util.getMD5Str(errorContent);
		// 直接保存原始错误日志，有前端进行URLDecoder
		// errorTitle = URLDecoder.decode(errorTitle, "utf8");
		// errorContent = URLDecoder.decode(errorContent, "utf8");

		/*
		 * try{ errorContent = GzipUtils.decompress(Base64Util.decodeFromString(errorContent)); }catch(Exception e){
		 * return; } errorContent = URLEncoder.encode(errorContent,"utf-8");
		 */
		if (errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH) {
			errorTitle = errorTitle.substring(0, UTF_MAX_LENGTH);
			errorContent = errorContent.substring(0, UTF_MAX_LENGTH);
		}

		// 错误详情
		mapKeyObj.setOutFields(new String[] { appID, appVersion, channel, country, province, errorMD5 });
		mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_ERROR_REPORT_DETAIL);
		mapValueObj.setOutFields(new String[] { errorTime, errorTitle, errorContent });
		context.write(mapKeyObj, mapValueObj);

		// 错误分布
		mapKeyObj.setOutFields(new String[] { appID, appVersion, channel, country, province, errorMD5, os, brand });
		mapKeyObj.setSuffix(Constants.SUFFIX_CHANNEL_ERROR_REPORT_DIST);
		mapValueObj.setOutFields(new String[] { errorTime, errorTitle, errorContent });
		context.write(mapKeyObj, mapValueObj);
	}
}
