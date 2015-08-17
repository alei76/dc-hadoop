package net.digitcube.hadoop.driver;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.model.UserInfoLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 对不同的自定义事件根据 ID 进行分离
 * 并且以自定义事件的 ID 作为输出后缀 
 *
 */
public class UIDCounterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text keyObj = new Text();

	private String fileName;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if(fileName.contains("UserInfo")){
			UserInfoLog userInfoLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				userInfoLog = new UserInfoLog(paraArr);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}
			
			String uid = userInfoLog.getUID();
			keyObj.set(uid);
			context.write(keyObj, NullWritable.get());
			
		}else if(fileName.contains("Online")){
			OnlineLog onlineLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				onlineLog = new OnlineLog(paraArr);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}
			String uid = onlineLog.getUID();
			keyObj.set(uid);
			context.write(keyObj, NullWritable.get());
		}else{
			context.write(value, NullWritable.get());
		}
		
	}
}
