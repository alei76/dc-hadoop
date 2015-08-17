package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlineDayCheckReducer extends Reducer<IntWritable, IntWritable, Text, NullWritable> {

	private TreeSet<Integer> set = new TreeSet<Integer>();
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		for(Integer i : set){
			cal.setTimeInMillis(1000L * i);
			context.write(new Text(sdf.format(cal.getTime())), NullWritable.get());
		}
	}

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for(IntWritable i : values){
			set.add(i.get());
		}
	}
}
