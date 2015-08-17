package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午1:53:33 @copyrigt www.digitcube.net<br>
 *          key = APPID,platform,gameregion,point <br>
 *          value = sum(1)
 */

/**
 * use @AcuPcuCntHourMapper and @AcuPcuCntHourReducer instead
 */
@Deprecated
public class ACUHourReducer
		extends
		Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable vaule = new IntWritable();
	
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
		
		vaule.set(total);
		key.setSuffix(Constants.SUFFIX_ACU_HOUR);
		context.write(key, vaule);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}