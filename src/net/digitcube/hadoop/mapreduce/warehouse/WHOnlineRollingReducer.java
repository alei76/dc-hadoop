package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHOnlineRollingMapper
 * 
 * @author sam.xie
 * @date 2015年6月30日 下午4:23:33
 * @version 1.0
 */
public class WHOnlineRollingReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int firstLoginDate = 0; // 取最小的Date
		for (OutFieldsBaseModel val : values) {
			int loginDate = StringUtil.convertInt(val.getOutFields()[0], 0);
			if (firstLoginDate == 0 || firstLoginDate > loginDate) {
				firstLoginDate = loginDate;
			}
		}
		valFields.setOutFields(new String[] { firstLoginDate + "" });
		context.write(key, valFields);
	}
}
