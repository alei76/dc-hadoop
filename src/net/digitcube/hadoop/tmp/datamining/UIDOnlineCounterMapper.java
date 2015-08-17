package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UIDOnlineCounterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text mapKeyObj = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


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
		
		/*//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}*/		
		
		mapKeyObj.set(onlineLog.getUID());
		context.write(mapKeyObj, NullWritable.get());
	
	}
}
