package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class AppList2Reducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel outValue = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String suffix = key.getSuffix();
		if (suffix.equals(Constants.SUFFIX_APPLIST_RINGTONE)) {
			// 其实就一个值
			for (OutFieldsBaseModel value : values) {
				context.write(key, value);
			}
		}
		if (suffix.equals(Constants.SUFFIX_APPLIST_APP)) {
			// 分别对 setuptime 和setTime 求和
			int sumUid = 0;
			int sumSetUpTime = 0;
			int sumUpTime = 0;
			for (OutFieldsBaseModel value : values) {
				sumUid += StringUtil.convertInt(value.getOutFields()[0], 0);
				sumSetUpTime += StringUtil.convertInt(value.getOutFields()[1], 0);
				sumUpTime += StringUtil.convertInt(value.getOutFields()[2], 0);
			}
			outValue.setOutFields(new String[] { sumUid + "", sumSetUpTime + "", sumUpTime + "" });
			context.write(key, outValue);
		}
	}
}
