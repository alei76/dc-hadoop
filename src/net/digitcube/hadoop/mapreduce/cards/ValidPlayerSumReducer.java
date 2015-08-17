package net.digitcube.hadoop.mapreduce.cards;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ValidPlayerSumReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable total = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int totalPlayer = 0;
		
		for (IntWritable val : values) {
			//totalPlayer += val.get();
			totalPlayer++;
		}
		total.set(totalPlayer);
		
		//key.setSuffix(Constants.SUFFIX_VALID_PLAYER_SUM);
		context.write(key, total);
	}

}
