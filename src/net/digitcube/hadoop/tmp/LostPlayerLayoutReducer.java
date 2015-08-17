package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LostPlayerLayoutReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int totalPlayerNum = 0;
		for(IntWritable val : values){
			totalPlayerNum+=val.get();
		}
		valObj.set(totalPlayerNum);
		context.write(key, valObj);
	}
}
