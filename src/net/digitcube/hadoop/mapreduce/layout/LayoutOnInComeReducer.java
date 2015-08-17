package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author rickpan
 * @version 1.0 
 * 收入按日统计
 * 
 * 输入：
 * key : appId, platform, channel, gameServer, 维度指标(如年龄, age), 维度值(15)
 * value : currencyAmount
 * 
 * 输出：
 * key : appId, platform, channel, gameServer, 维度指标(如年龄, age), 维度值(15)
 * value : sum( currencyAmount )
 * 
 */

public class LayoutOnInComeReducer extends Reducer<OutFieldsBaseModel, FloatWritable, OutFieldsBaseModel, FloatWritable> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

		float total = 0;
		for (FloatWritable val : values) {
			total += val.get();
		}
		
		key.setSuffix(Constants.SUFFIX_LAYOUT_ON_INCOME);
		
		context.write(key, new FloatWritable(total));
	}

}
