package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CHDownloadForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String DATA_FLAG_EVENT = "E";
	public static final String DATA_FLAG_ONLINE = "O";
	
	private OutFieldsBaseModel mapKey = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapVal = new OutFieldsBaseModel();

	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		//if(fileSuffix.contains(Constants.DESELF_CHANNEL_RES_DOWNLOAD)){
		if(fileSuffix.endsWith("DESelf_Channel_Res_Download")){
			EventLog2 eventLog;
			try {
				// 日志里某些字段有换行字符，导致这条日志换行而不完整，
				// 用 try-catch 过滤掉这类型的错误日志
				eventLog = new EventLog2(paramArr);
			} catch (Exception e) {
				return;
			}
			String appId = eventLog.getAppID();
			String uid = eventLog.getUID();
			
			String[] keyFields = new String[]{
					appId,
					uid
			};
			mapKey.setOutFields(keyFields);
			mapVal.setSuffix(DATA_FLAG_EVENT);
			mapVal.setOutFields(paramArr);
			context.write(mapKey, mapVal);
			
		}else if(fileSuffix.contains(Constants.SUFFIX_CHANNEL_ONLINE_INFO)){
			OnlineDayLog2 onlineDayLog2 = new OnlineDayLog2(paramArr);
			String appId = onlineDayLog2.getAppId();
			String uid = onlineDayLog2.getUid();
			
			String[] keyFields = new String[]{
					appId,
					uid
			};
			mapKey.setOutFields(keyFields);
			mapVal.setSuffix(DATA_FLAG_ONLINE);
			mapVal.setOutFields(paramArr);
			context.write(mapKey, mapVal);
		}
	}
}
