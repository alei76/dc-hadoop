package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 资源位点击/曝光
 * 
 * -----------------------------输入-----------------------------
 * 1.资源位曝光量
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RL_SHOW
 * Value:one
 * 
 * 2.资源位点击量
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RL_CLICK
 * Value:			one
 * 
 * -----------------------------输出-----------------------------
 * 1.资源位曝光量汇总
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RL_SHOW
 * Value:			sum
 * 
 * 2.资源位点击数汇总
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RL_CLICK
 * Value:			sum
 * 
 * 
 * @author sam.xie
 * @date 2015年3月3日 下午5:56:50
 * @version 1.0
 */
public class CHResourceLocationReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	private IntWritable sum = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		String suffix = key.getSuffix();
		if (Constants.SUFFIX_CHANNEL_RL_CLICK.equals(suffix) || Constants.SUFFIX_CHANNEL_RL_SHOW.equals(suffix)) {
			int tmp = 0;
			for (IntWritable intWritable : values) {
				tmp += intWritable.get();
			}
			sum.set(tmp);
			context.write(key, sum);
		}
	}
}
