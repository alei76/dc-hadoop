package net.digitcube.hadoop.mapreduce.errorreport;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.ErrorReportLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang
 * 
 */
public class ErrorReportMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	private final static int UTF_MAX_LENGTH = 65535;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] errorReportArr = value.toString().split(MRConstants.SEPERATOR_IN);
		ErrorReportLog errorReport = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			errorReport = new ErrorReportLog(errorReportArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(errorReport.getAppID())){
			return;
		}
		
		String appId = errorReport.getAppID();
		String platform = errorReport.getPlatform();

		String opSystem = errorReport.getOperSystem();
		String brand = errorReport.getBrand();

		String errorTime = ""+errorReport.getErrorTime();
		String errorMD5 = errorReport.getMD5();
		String errorTitle = errorReport.getTitle();
		String errorContent = errorReport.getContent();
		
		// 当 errorTitle 或  errorContent 字节数大于 65535 时
		// OutFieldsBaseModel 中的 writeUTF 方法不支持，数据库对应的字段(text类型)也不支持
		// 所以需修改数据库字段长度，同时修改 writeUTF 为 write(byte[] b)(使用OutFieldsBaseModel2)
		// 目前暂且把这类型错误过滤(这类型错误属于极少数)
		if(errorTitle.getBytes().length > UTF_MAX_LENGTH || errorContent.getBytes().length > UTF_MAX_LENGTH){
			return;
		}
		
		String[] keyFields = null;
		String[] valueFields = null;

		// A..输出错误详情
		keyFields = new String[] { appId, platform, errorMD5 };
		valueFields = new String[] { "A", errorTime, errorTitle, errorContent };
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		context.write(mapKeyObj, mapValueObj);

		// B..输出错误分布计数
		keyFields = new String[] { appId, platform, errorMD5, opSystem, brand };
		valueFields = new String[] { "B" };
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		context.write(mapKeyObj, mapValueObj);
		
	}
}
