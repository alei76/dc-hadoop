package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月19日 下午4:52:48 @copyrigt www.digitcube.net<br>
 *          输入：Key:APPID PlatForm Type value:1<br>
 *          输出：APPID PlatForm Type sum(1)
 */

public class UserFlowReducer
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
		key.setSuffix(Constants.SUFFIX_USERFLOW_APP);
		context.write(key, new IntWritable(total));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
