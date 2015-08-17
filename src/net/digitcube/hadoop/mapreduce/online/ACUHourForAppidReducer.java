package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午2:21:30 @copyrigt www.digitcube.net <br>
 * <br>
 *          输入: key = APPID,platform,gameregion value = onlinenum在线人数<br>
 *          输出： APPID,platform,gameregion,AVG(onlinenum)<br>
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class ACUHourForAppidReducer
		extends
		Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, FloatWritable> {
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		// 计算每个APPID|Platform|5-MinutePoint的和
		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		key.setSuffix(Constants.SUFFIX_ACU_HOUR_APP);
		context.write(key, new FloatWritable(total / 12));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
