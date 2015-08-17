package net.digitcube.hadoop.mapreduce.compress;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Bzip2CompressDayReducer extends Reducer<Text, BigFieldsBaseModel, BigFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<BigFieldsBaseModel> values, Context context) 
			throws IOException, InterruptedException {
		
		String suffix = key.toString();
		for(BigFieldsBaseModel val : values){
			val.setSuffix(suffix);
			context.write(val, NullWritable.get());
		}
	}
}
