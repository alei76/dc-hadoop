package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHAccountDayMapper
 * 
 * @author sam.xie
 * @date 2015年7月13日 下午4:23:33
 * @version 1.0
 */
public class WHAccountDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int maxLevel = 0;
		for (OutFieldsBaseModel val : values) {
			int level = StringUtil.convertInt(val.getOutFields()[0], 0);
			maxLevel = maxLevel < level ? level : maxLevel;
		}
		valFields.setOutFields(new String[] { maxLevel + "" });
		context.write(key, valFields);
	}
}
