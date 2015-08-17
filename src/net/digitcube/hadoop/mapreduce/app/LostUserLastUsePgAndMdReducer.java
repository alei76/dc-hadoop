package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LostUserLastUsePgAndMdReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable valObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int totalCount=0;
		for(IntWritable count : values){
			totalCount += count.get();
		}
		valObj.set(totalCount);
		
		key.setSuffix(Constants.SUFFIX_APP_LOSTUSER_LAST_USE_PG_MD);
		context.write(key, valObj);
	}
}
