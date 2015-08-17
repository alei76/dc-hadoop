package net.digitcube.hadoop.driver;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.MRConstants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 对不同的自定义事件根据 ID 进行分离
 * 并且以自定义事件的 ID 作为输出后缀 
 *
 */
public class PersonMapper extends Mapper<LongWritable, Text, BigFieldsBaseModel, BigFieldsBaseModel> {

	private BigFieldsBaseModel keyObj = new BigFieldsBaseModel();
	private BigFieldsBaseModel valObj = new BigFieldsBaseModel();

	private String fileName;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String name = arr[i++];
		String age = arr[i++];
		String counts = arr[i++];
		String money = arr[i++];
		
		String[] keyFields = new String[]{
				name,
				age
		};
		
		String[] valFields = new String[]{
				counts,
				money
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
}

