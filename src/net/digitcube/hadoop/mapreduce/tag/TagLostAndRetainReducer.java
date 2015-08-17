package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TagLostAndRetainReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int totalPlayers = 0;
		for(IntWritable val : values){
			totalPlayers += val.get();
		}
		
		valObj.set(totalPlayers);
		context.write(key, valObj);
	}
}
