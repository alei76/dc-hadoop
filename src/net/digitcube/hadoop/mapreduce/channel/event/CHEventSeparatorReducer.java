package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 将Mapper阶段的零散文件合并
 * see @EventSeparator3Mapper
 */

public class CHEventSeparatorReducer extends Reducer<Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException,
			InterruptedException {

		String suffix = key.toString();
		for (OutFieldsBaseModel val : values) {
			val.setSuffix(suffix);
			context.write(val, NullWritable.get());
		}
	}
}
