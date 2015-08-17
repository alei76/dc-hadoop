package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 资源搜索
 * 
 * -----------------------------输入-----------------------------
 * 1.资源搜索关键字
 * Key:				appID,appVersion,channel,country,province,keyword
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RES_SEARCH
 * Value:			one
 * 
 * -----------------------------输出-----------------------------
 * 1.资源位搜索关键字统计
 * Key:				appID,appVersion,channel,country,province,keyword,total
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RES_SEARCH
 * Value:			sum
 * 
 * @author sam.xie
 * @date 2015年3月4日 下午3:56:50
 * @version 1.0
 */
public class CHResourceSearchReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	private IntWritable sum = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		String suffix = key.getSuffix();
		if (Constants.SUFFIX_CHANNEL_RES_SEARCH.equals(suffix)) {
			int tmp = 0;
			for (IntWritable intWritable : values) {
				tmp += intWritable.get();
			}
			sum.set(tmp);
			context.write(key, sum);
		}
	}
}
