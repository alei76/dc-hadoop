package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see TmpWHMapper
 */
public class TmpWHReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		for (OutFieldsBaseModel val : values) {
			context.write(key, val);
		}
	}
}
