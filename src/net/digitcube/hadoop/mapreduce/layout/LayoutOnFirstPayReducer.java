package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author rickpan
 * @version 1.0 
 * 首付分布统计
 * 首付时游戏天数、游戏时长、首付金额、首付等级
 * 
 */

public class LayoutOnFirstPayReducer extends Reducer<OutFieldsBaseModel, FloatWritable, OutFieldsBaseModel, FloatWritable> {
	
	private FloatWritable val = new FloatWritable();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

		float total = 0;
		for (FloatWritable val : values) {
			total += val.get();
		}
		
		val.set(total);
		context.write(key, val);
	}

}
