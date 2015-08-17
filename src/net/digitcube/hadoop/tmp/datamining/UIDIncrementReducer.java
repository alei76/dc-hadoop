package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UIDIncrementReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable keyObj = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int total = 0;
		for(IntWritable count : values){
			total += count.get();
		}
		keyObj.set(total);
		context.write(key, keyObj);
	}
}
