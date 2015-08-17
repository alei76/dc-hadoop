package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKBrandReducer extends Reducer<Text, IntWritable, Text, Text> {

	Text keyObj = new Text();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int total = 0;
		for(IntWritable count : values){
			total += count.get();
		}
		String count = String.format("%09d", total);
		
		keyObj.set(count);
		context.write(keyObj, key);
	}
}
