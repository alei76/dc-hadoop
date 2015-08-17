package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see TmpWHRollingMergeMapper
 */
public class TmpWHRollingMergeReducer extends
		Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		context.write(key, NullWritable.get());
		// String uid = key.getOutFields()[0];
		// String appId = key.getOutFields()[1];
		//
		// String[] resultArray = null;
		// String firstPlatform = "-";
		// String firstChannel = "-";
		// int firstTime = 0;
		// for (OutFieldsBaseModel val : values) {
		// String[] valArray = val.getOutFields();
		// if ("R".equals(val.getSuffix())) {
		// resultArray = valArray;
		// } else if ("D".equals(val.getSuffix())) {
		// int ts = StringUtil.convertInt(valArray[0], 0);
		// String platform = valArray[1];
		// String channel = valArray[2];
		// if (firstTime <= 0 || firstTime > ts) {
		// firstTime = ts;
		// firstPlatform = platform;
		// firstChannel = channel;
		// }
		// }
		// }
		// try {
		// if (null != resultArray) {
		// String[] keyArray = new String[] { uid, appId, firstPlatform, firstChannel };
		// key.setOutFields(keyArray);
		// key.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING);
		// valFields.setOutFields(resultArray);
		// context.write(key, valFields);
		// }
		//
		// } catch (Exception e) {
		// System.err.println("key:" + Arrays.toString(key.getOutFields()) + ",value:"
		// + Arrays.toString(resultArray));
		// }
	}
}
