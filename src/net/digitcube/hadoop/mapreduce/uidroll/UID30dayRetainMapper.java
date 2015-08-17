package net.digitcube.hadoop.mapreduce.uidroll;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class UID30dayRetainMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = paraArr[i++];
		String version = paraArr[i++];
		String platform = paraArr[i++];
		String channel = paraArr[i++];
		String gameserver = paraArr[i++];
		String uid = paraArr[i++];
		String accountId = paraArr[i++];
		
		if(fileSuffix.endsWith(Constants.SUFFIX_UID_NEWADD_DAY)){
			String[] keyFields = new String[]{
					appId,
					version,
					platform,
					channel,
					gameserver
			};
			
			mapKeyObj.setSuffix(Constants.SUFFIX_UID_NEWADD_DAY_SUM);
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, one);
			
		}else if(fileSuffix.endsWith(Constants.SUFFIX_UID_30DAY_RETAIN)){
			String dayOfSet = paraArr[i++];
			String[] keyFields = new String[]{
					appId,
					version,
					platform,
					channel,
					gameserver,
					dayOfSet
			};
			mapKeyObj.setSuffix(Constants.SUFFIX_UID_30DAY_RETAIN_SUM);
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, one);
		}
	}
}
