package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AppDownloadTimeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	IntWritable total = new IntWritable();
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int counter = 0;
		for(IntWritable val : values){
			counter += val.get();
		}
		total.set(counter);
		context.write(key, total);
	}
}
