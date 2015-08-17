package net.digitcube.hadoop.mapreduce.gsroll;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GSRollInfoSumReducer extends Reducer<OutFieldsBaseModel, FloatWritable, OutFieldsBaseModel, FloatWritable> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

		float total = 0;
		for (FloatWritable val : values) {
			total += val.get();
		}
		context.write(key, new FloatWritable(total));
	}
}
