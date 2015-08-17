package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GuankaPayAndItemReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, LongWritable> {
	
	private LongWritable valObj = new LongWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		long total = 0;
		for(IntWritable val : values){
			total += val.get();
		}
		
		valObj.set(total);
		context.write(key, valObj);
	}
}
