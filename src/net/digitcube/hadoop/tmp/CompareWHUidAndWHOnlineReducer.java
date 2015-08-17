package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CompareWHUidAndWHOnlineReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] paramUid = null;
		String[] paramDevice = null;
		for (OutFieldsBaseModel val : values) {
			if ("UID".equals(val.getSuffix())) {
				paramUid = val.getOutFields();
			} else if ("DEV".equals(val.getSuffix())) {
				paramDevice = val.getOutFields();
			}
		}
		if (null == paramUid && null != paramDevice) {
			key.setSuffix("DEV");
			key.setOutFields(paramDevice);
			context.write(key, NullWritable.get());
		} else if (null != paramUid && null == paramDevice) {
			key.setSuffix("UID");
			key.setOutFields(paramUid);
			context.write(key, NullWritable.get());
		}
	}
}
