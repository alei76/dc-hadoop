package net.digitcube.hadoop.mapreduce.event;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.IOSChannelUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 对不同的自定义事件根据 ID 进行分离
 * 并且以自定义事件的 ID 作为输出后缀 
 *
 */
public class EventSeparatorMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();

	private String filePath = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Added at 20140808
		//某些字段中存在换行符，导致计算出错，需去掉
		String line = value.toString().replace("\r", "").replace("\n", "");
		String[] array = line.split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(array);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
			return;
		}
		
		String eventID = eventLog.getEventId();
		eventID = eventID.contains(":") ? eventID.split(":")[0] : eventID;
		eventID = eventID.startsWith("_") ? eventID.replaceFirst("_", "") : eventID;
		
		keyObj.setSuffix(eventID);
		//keyObj.setOutFields(array);
		
		//Added at 20140606 : iOS 渠道修正
		//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		//if(MRConstants.PLATFORM_iOS_STR.equals(eventLog.getPlatform())){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(eventLog.getAppID(), 
																eventLog.getUID(), 
																eventLog.getPlatform(), 
																eventLog.getChannel());
			eventLog.setChannel(reviseChannel);
		//}
		
		keyObj.setOutFields(eventLog.toOldVersionArr());
		
		//临时兼容 html5 的事件
		if(filePath.contains("logserver-js")){
			keyObj.setOutFields(array);
		}
		
		context.write(keyObj, NullWritable.get());
	}
}
