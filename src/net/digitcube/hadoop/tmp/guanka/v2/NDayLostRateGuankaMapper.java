package net.digitcube.hadoop.tmp.guanka.v2;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NDayLostRateGuankaMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		int i = 0;
		String pureAppId = arr[i++];
		String accountId = arr[i++];
		String maxGuanKa = arr[i++];
		String newFlag = arr[i++];
		String lostFlag = arr[i++];
		
		String[] keyFields = new String[]{
				pureAppId,
				"关卡"+maxGuanKa
		};
		String[] valFields = new String[]{
				newFlag,
				lostFlag
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
	}
}
