package net.digitcube.hadoop.mapreduce.ext;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemSumReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, LongWritable> {
	
	private LongWritable valObj = new LongWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		long total = 0;
		for(IntWritable val : values){
			total += val.get();
		}
		valObj.set(total);
		key.setSuffix(Constants.SUFFIX_ITEM_SUM);
		context.write(key, valObj);
	}
}
