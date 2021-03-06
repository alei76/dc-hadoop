package net.digitcube.hadoop.mapreduce.habits;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 当日/前7/前30天活跃玩家数，首日/周/月付费数
 * 
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月30日 上午11:17:18 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserPay1730Reducer
		extends
		Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		key.setSuffix(Constants.SUFFIX_1730_ACT_PAY);
		context.write(key, new IntWritable(total));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
