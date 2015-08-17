package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author 
 * @version 
 * 
 * 输入：
 * key : appId, platform, channel, gameServer, playerType, 维度指标(如联网方式, netType), 维度值(3G)
 * value : 1
 * 
 * 输出：
 * key : appId, platform, channel, gameServer, playerType, 维度指标(如联网方式, netType), 维度值(3G)
 * value : sum(1)
 * 
 */

public class PlayerLayoutReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	private IntWritable valObj = new IntWritable();
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		valObj.set(total);
		
		context.write(key, valObj);
	}

}
