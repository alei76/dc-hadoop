package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * <pre>
 * 对PhoneAppReducer的输出进行进一步的计算
 * @author Ivan          <br>
 * @date 2014-6-21 下午4:53:41 <br>
 * @version 1.0
 * <br>
 */
public class PhoneApp2Mapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private String fileSuffix;
	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		// 应用信息
		String appID = lineArr[0];
		String version = lineArr[1];
		String platform = lineArr[2];
		String channel = lineArr[3];
		String gameserver = lineArr[4];
		String appName = lineArr[5];
		String[] keyArr = new String[] { appID, version, platform, channel, gameserver, appName };
		outputKey.setOutFields(keyArr);
		if (fileSuffix.endsWith(Constants.SUFFIX_APPLIST_APP)) {
			// appID version platform channel gameserver appName 1 setuptimes uptime
			outputKey.setSuffix(Constants.SUFFIX_APPLIST_APP);
			String[] valueArr = new String[] { lineArr[6], lineArr[7], lineArr[8] };
			outputValue.setOutFields(valueArr);
			context.write(outputKey, outputValue);
		}
		if (fileSuffix.endsWith(Constants.SUFFIX_APPLIST_RINGTONE)) {
			// appID version platform channel gameserver appName 1
			outputKey.setSuffix(Constants.SUFFIX_APPLIST_RINGTONE);
			String[] valueArr = new String[] { lineArr[6] };
			outputValue.setOutFields(valueArr);
			context.write(outputKey, outputValue);
		}
	}
}
