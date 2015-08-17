package net.digitcube.hadoop.mapreduce.userroll.week;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeekUserFlowMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String suffix = "";
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(fileName.contains(Constants.SUFFIX_USER_LOST_WEEK_1)){
			suffix = Constants.SUFFIX_USER_LOST_WEEK_1;
		}else if(fileName.contains(Constants.SUFFIX_USER_BACK_WEEK_1)){
			suffix = Constants.SUFFIX_USER_BACK_WEEK_1;
		}
		mapKeyObj.setSuffix(suffix);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String statTime = paraArr[i++];
		String appId = paraArr[i++];
		String platform = paraArr[i++];
		String channel = paraArr[i++];
		String gameServer = paraArr[i++];
		String accountId = paraArr[i++];
		String playerType = paraArr[i++];
		String[] outFields = new String[]{
				statTime,
				appId,
				platform,
				channel,
				gameServer,
				playerType
		};
		mapKeyObj.setOutFields(outFields);
		context.write(mapKeyObj, one);
	}
}
