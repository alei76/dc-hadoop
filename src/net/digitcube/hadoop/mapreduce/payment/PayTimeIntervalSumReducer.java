package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * see @PayTimeIntervalSumMapper		
 */

public class PayTimeIntervalSumReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable redValObj = new IntWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int totalPlayerCount = 0;
		
		for (IntWritable val : values) {
			totalPlayerCount += val.get();
		}
		
		key.setSuffix(Constants.SUFFIX_PAY_TIME_INTERVAL_SUM);
		redValObj.set(totalPlayerCount);
		context.write(key, redValObj);
	}

}
