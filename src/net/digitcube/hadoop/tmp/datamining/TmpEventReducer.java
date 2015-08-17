package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TmpEventReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	IntWritable total = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		context.write(key, NullWritable.get());
	}
}
