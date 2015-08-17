package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AppViewAndDownloadReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int totalTimes = 0;
		for(IntWritable value : values){
			totalTimes += value.get();
		}
		
		key.setSuffix(Constants.SUFFIX_APP_VIEW_AND_DOWNLOAD);
		valObj.set(totalTimes);
		
		context.write(key, valObj);
	}
}
