package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Key=appID|platform|channel|gameServer|type|userType|roomId
 * Value=sum(1)
 * @author mikefeng
 *
 */
public class RoomPlayerCountReducer extends
					Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, LongWritable> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {		
		long sum = 0;
		for(IntWritable value : values){
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));		
	}
	
	
}
