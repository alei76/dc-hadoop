package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TmpLevelLayoutReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, IntWritable> {

	IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		int totalPlayerNum = 0;
		for(NullWritable val : values){
			totalPlayerNum++;
		}
		valObj.set(totalPlayerNum);
		context.write(key, valObj);
	}
}
