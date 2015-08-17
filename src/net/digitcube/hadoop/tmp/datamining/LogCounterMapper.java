package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 统计每天的日志量：大小和行数
 */
public class LogCounterMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	String fileName = "";
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!fileName.contains("Online")){
			return;
		}
		
		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(onlineArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}
		/*
		String pureAppId = onlineLog.getAppID().split("\\|")[0];
		String platform = onlineLog.getPlatform();
		String accountId = onlineLog.getAccountID();
		keyObj.setOutFields(new String[]{
				pureAppId,
				platform,
				accountId
		});*/
		
		String pureAppId = onlineLog.getAppID().split("\\|")[0];
		String accountId = onlineLog.getAccountID();
		keyObj.setOutFields(new String[]{
				
				"IMDI"
		});
		keyObj.setSuffix("DATE");
		context.write(keyObj, NullWritable.get());
	}
}
