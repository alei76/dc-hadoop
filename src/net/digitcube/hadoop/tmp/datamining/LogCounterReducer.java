package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LogCounterReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		key.setSuffix("DAU");
		context.write(key, NullWritable.get());
	}
}
