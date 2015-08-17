package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class H5PlayerInfoLayoutReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	
	private IntWritable valObj = new IntWritable();
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		valObj.set(total);
		context.write(key, valObj);
	}
}