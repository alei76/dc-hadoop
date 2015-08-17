package net.digitcube.hadoop.mapreduce.errorreport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MD5Util;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.ErrorReportLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.Base64Util;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.GzipUtils;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 注意：
 * 20150210 和彭俊修强确认错误日志的兼容
 * a）eventID 是 _DESelf_UserDefined_ErrorReport
 * 那么不管平台是什么都对 title 和 content 做 URL Encode
 * b）eventID 是 _DESelf_UserDefined_ErrorReport_2
 * 如果平台是 ios，那么对 title 和 content 做 URL Encode
 * 如果平台是 安卓，保持不变
 * c）后面将统一使用 _DESelf_UserDefined_ErrorReport_3 作为 eventID
 */
public class ErrorReportDayMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private BigFieldsBaseModel mapValueObj = new BigFieldsBaseModel();
	
	private final static int UTF_MAX_LENGTH = 65535;
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if (fileSuffix.startsWith(Constants.ErrorReport)) {
			String[] errorReportArr = value.toString().split(MRConstants.SEPERATOR_IN);
			ErrorReportLog errorReport = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				errorReport = new ErrorReportLog(errorReportArr);
			}catch(Exception e){
				return;
			}
			
			//验证appId长度 add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(errorReport.getAppID())){
				return;
			}
			
			String appId = errorReport.getAppID();
			String platform = errorReport.getPlatform();
			String channel = errorReport.getChannel();
			String gameServer= errorReport.getGameServer();
			String opSystem = errorReport.getOperSystem();
			String brand = errorReport.getBrand();	
			String errorTime = ""+errorReport.getErrorTime();
			String errorTitle = errorReport.getTitle();
			String errorContent = errorReport.getContent();	
			if(StringUtil.isEmpty(errorContent) || StringUtil.isEmpty(errorTitle)
					|| MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(errorContent)
					|| MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(errorTitle)){
				return;
			}			
			try{
				errorContent = GzipUtils.decompress(Base64Util.decodeFromString(errorContent));
			}catch(Exception e){
				return;
			}			
			String errorMD5 = MD5Util.getMD5Str(errorContent);
			errorContent = URLEncoder.encode(errorContent,"utf-8");	
			
			if(errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH){
				return;
			}
			
			String valueSuffix = "SYS";
		
			writeOutput(appId, platform, channel, gameServer,
						errorMD5, errorTime, errorTitle, errorContent,
						opSystem, brand, valueSuffix, context);
			
		}else if(fileSuffix.endsWith(Constants.DESelf_UserDefined_ErrorReport)){
			String[] eventReportArr = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = null;			
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				eventLog = new EventLog(eventReportArr);
			}catch(Exception e){
				return;
			}			
			
			//验证appId长度 add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
				return;
			}			
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer= eventLog.getGameServer();
			String opSystem = eventLog.getOperSystem();
			String brand = eventLog.getBrand();	
			String errorTime = ""+eventLog.getArrtMap().get("errorTime");			
			String errorTitle = eventLog.getArrtMap().get("title");
			String errorContent = eventLog.getArrtMap().get("content");
			if(StringUtil.isEmpty(errorContent) || StringUtil.isEmpty(errorTitle)){
				return;
			}			
			//eventID-->DESelf_UserDefined_ErrorReport 时
			//需 URL Encode title 和 content
			errorTitle = URLEncoder.encode(errorTitle, "UTF-8");
			errorContent = URLEncoder.encode(errorContent, "UTF-8");
			String errorMD5 = MD5Util.getMD5Str(errorContent);		
			if(errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH){
				return;
			}
			
			String valueSuffix = "USER";
			
			writeOutput(appId, platform, channel, gameServer,
					errorMD5, errorTime, errorTitle, errorContent,
					opSystem, brand, valueSuffix, context);
			
		}else if(fileSuffix.endsWith(Constants.DESelf_UserDefined_ErrorReport_2)){
			String[] eventReportArr = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = null;			
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				eventLog = new EventLog(eventReportArr);
			}catch(Exception e){
				return;
			}			
			
			//验证appId长度 add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
				return;
			}			
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer= eventLog.getGameServer();
			String opSystem = eventLog.getOperSystem();
			String brand = eventLog.getBrand();	
			String errorTime = ""+eventLog.getArrtMap().get("errorTime");			
			String errorTitle = eventLog.getArrtMap().get("title");
			String errorContent = eventLog.getArrtMap().get("content");
			if(StringUtil.isEmpty(errorContent) || StringUtil.isEmpty(errorTitle)){
				return;
			}				
			
			//eventID-->DESelf_UserDefined_ErrorReport_2 并且平台为 ios 时
			//需 URL Encode title 和 content
			if(MRConstants.PLATFORM_iOS_STR.equals(platform)){
				errorTitle = URLEncoder.encode(errorTitle, "UTF-8");
				errorContent = URLEncoder.encode(errorContent, "UTF-8");
			}
			String errorMD5 = MD5Util.getMD5Str(errorContent);		
			if(errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH){
				return;
			}
			
			String valueSuffix = "USER";
			
			writeOutput(appId, platform, channel, gameServer,
					errorMD5, errorTime, errorTitle, errorContent,
					opSystem, brand, valueSuffix, context);
		}else if(fileSuffix.endsWith(Constants.DESelf_UserDefined_ErrorReport_3)){
			String[] eventReportArr = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = null;			
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				eventLog = new EventLog(eventReportArr);
			}catch(Exception e){
				return;
			}			
			
			//验证appId长度 add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
				return;
			}			
			String appId = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer= eventLog.getGameServer();
			String opSystem = eventLog.getOperSystem();
			String brand = eventLog.getBrand();	
			String errorTime = ""+eventLog.getArrtMap().get("errorTime");			
			String errorTitle = eventLog.getArrtMap().get("title");
			String errorContent = eventLog.getArrtMap().get("content");
			if(StringUtil.isEmpty(errorContent) || StringUtil.isEmpty(errorTitle)){
				return;
			}				
			
			//eventID-->DESelf_UserDefined_ErrorReport_2 并且平台为 ios 时
			//需 URL Encode title 和 content
			// DESelf_UserDefined_ErrorReport_3已经对ios做了URL Encode，这里不需要再处理
//			if(MRConstants.PLATFORM_iOS_STR.equals(platform)){
//				errorTitle = URLEncoder.encode(errorTitle, "UTF-8");
//				errorContent = URLEncoder.encode(errorContent, "UTF-8");
//			}
			String errorMD5 = MD5Util.getMD5Str(errorContent);		
			if(errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH){
				return;
			}
			
			String valueSuffix = "USER";
			
			writeOutput(appId, platform, channel, gameServer,
					errorMD5, errorTime, errorTitle, errorContent,
					opSystem, brand, valueSuffix, context);
		}		
	}

	private void writeOutput( 
			String appId, 
			String platform,
			String channel,
			String gameServer,
			String errorMD5,
			String errorTime, 
			String errorTitle, 
			String errorContent,
			String opSystem, 
			String brand,
			String valueSuffix,
			Context context) throws IOException, InterruptedException{
		
		// A..输出错误详情
		String[] keyFields = new String[] { appId, platform, channel, gameServer, errorMD5 };
		String[] valueFields = new String[] { "A", errorTime, errorTitle, errorContent };
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		mapValueObj.setSuffix(valueSuffix);
		context.write(mapKeyObj, mapValueObj);
		
		// B..输出错误分布计数
		keyFields = new String[] { appId, platform, channel, gameServer, errorMD5, opSystem, brand };
		valueFields = new String[] { "B" };
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		mapValueObj.setSuffix(valueSuffix);
		context.write(mapKeyObj, mapValueObj);
	}
	
	public static void main(String[] args) throws Exception{
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\error_content.txt"));
		while(null != (line = br.readLine())){
			String[] eventReportArr = line.split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = null;			
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				eventLog = new EventLog(eventReportArr);
			}catch(Exception e){
				return;
			}
			String s =  eventLog.getArrtMap().get("content");
			System.out.println(s);
			String ds = URLDecoder.decode(s,"UTF-8");
			System.out.println("ds="+ds);
			String es = URLEncoder.encode(s,"UTF-8");
			System.out.println("es="+es);
		}
		br.close();
	}
}
