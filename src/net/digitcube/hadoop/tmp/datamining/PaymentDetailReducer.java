package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 付费明细
 */
public class PaymentDetailReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		for (OutFieldsBaseModel val : values) {
			val.setSuffix(key.getSuffix());
			context.write(val, NullWritable.get());
		}
	}
}
