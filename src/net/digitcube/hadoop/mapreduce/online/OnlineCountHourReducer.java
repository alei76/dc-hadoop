package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author 
 * @version 
 * 		输入：
 * 			key = APPID,platform,gameregion
 * 			value = 1
 * 		输出：
 *          key = APPID,platform,gameregion <br>
 *          value = sum(1)
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class OnlineCountHourReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable outVaule = new IntWritable();
	
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
		
		outVaule.set(total);
		key.setSuffix(Constants.SUFFIX_PLAYER_COUNT_HOUR_APP);
		context.write(key, outVaule);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}