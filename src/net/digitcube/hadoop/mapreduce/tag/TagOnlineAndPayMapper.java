package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TagOnlineAndPayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String DATA_FLAG_TAG_ONLINE = "O";
	public static final String DATA_FLAG_TAG_PAYMENT = "P";
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		if(fileSuffix.endsWith(Constants.SUFFIX_TAG_ONLINE_DAY)){
			String appId = paraArr[i++];
			String appVer = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String parTagName = paraArr[i++];
			String subTagName = paraArr[i++];
			String loginTimes = paraArr[i++];
			String onlineTime = paraArr[i++];
			String level = paraArr[i++];
			
			String[] keyFields = new String[]{
					appId,
					appVer,
					platform,
					channel,
					gameServer,
					parTagName,
					subTagName
			};
			String[] valFields = new String[]{
					loginTimes,
					onlineTime,
					level
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_TAG_ONLINE);
			context.write(keyObj, valObj);
			
		}else if(fileSuffix.endsWith(Constants.SUFFIX_TAG_PAYMENT_DAY)){
		
			String appId = paraArr[i++];
			String appVer = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String parTagName = paraArr[i++];
			String subTagName = paraArr[i++];
			String payTimes = paraArr[i++];
			String payAmount = paraArr[i++];
			
			String[] keyFields = new String[]{
					appId,
					appVer,
					platform,
					channel,
					gameServer,
					parTagName,
					subTagName
			};
			String[] valFields = new String[]{
					payTimes,
					payAmount
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_TAG_PAYMENT);
			context.write(keyObj, valObj);
		}
	}
}
