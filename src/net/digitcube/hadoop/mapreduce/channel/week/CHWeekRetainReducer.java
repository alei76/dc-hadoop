package net.digitcube.hadoop.mapreduce.channel.week;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHWeekRetainReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int totalPlayer = 0;
		for(IntWritable val : values){
			totalPlayer += val.get();
		}
		
		valObj.set(totalPlayer);
		key.setSuffix(Constants.SUFFIX_CHANNEL_WEEK_RETAIN);
		context.write(key, valObj);
	}
}
