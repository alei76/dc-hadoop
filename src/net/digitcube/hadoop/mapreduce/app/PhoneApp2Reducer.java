package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <pre>
 * 
 * @author Ivan          <br>
 * @date 2014-6-21 下午4:53:41 <br>
 * @version 1.0
 * <br>
 */
public class PhoneApp2Reducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String suffix = key.getSuffix();
		long uidSum = 0;
		long setupTimes = 0;
		long upTimes = 0;
		if (Constants.SUFFIX_APPLIST_APP.equals(suffix)) {
			for (OutFieldsBaseModel outFieldsBaseModel : values) {
				String[] valueArr = outFieldsBaseModel.getOutFields();
				uidSum += StringUtil.convertLong(valueArr[0], 0);
				setupTimes += StringUtil.convertLong(valueArr[1], 0);
				upTimes += StringUtil.convertLong(valueArr[2], 0);
			}
			String[] valueArr = new String[] { uidSum + "", setupTimes + "", upTimes + "" };
			outputValue.setOutFields(valueArr);
			context.write(key, outputValue);
		}
		if (Constants.SUFFIX_APPLIST_RINGTONE.equals(suffix)) {
			for (OutFieldsBaseModel outFieldsBaseModel : values) {
				String[] valueArr = outFieldsBaseModel.getOutFields();
				uidSum += StringUtil.convertLong(valueArr[0], 0);
			}
			String[] valueArr = new String[] { uidSum + "" };
			outputValue.setOutFields(valueArr);
			context.write(key, outputValue);
		}
	}
}
