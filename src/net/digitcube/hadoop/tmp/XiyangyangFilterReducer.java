package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class XiyangyangFilterReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
