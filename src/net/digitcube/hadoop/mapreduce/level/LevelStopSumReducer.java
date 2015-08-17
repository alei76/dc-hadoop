package net.digitcube.hadoop.mapreduce.level;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LevelStopSumReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	private IntWritable valObj = new IntWritable();
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		valObj.set(total);
		
		key.setSuffix("Level_Stop_Sum");
		
		context.write(key, valObj);
	}
}
