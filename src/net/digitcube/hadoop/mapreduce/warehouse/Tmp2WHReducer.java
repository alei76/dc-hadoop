package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see Tmp2WHMapper
 */
public class Tmp2WHReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		String[] finalLocation = null;
		for (OutFieldsBaseModel val : values) {
			String[] valArr = val.getOutFields();
			if ("L".equals(val.getSuffix())) {
				finalLocation = valArr;
			} else if ("H".equals(val.getSuffix())) {
				key.setOutFields(valArr);
				key.setSuffix("WAREHOUSE_TMP_HOUR");
				context.write(key, NullWritable.get());
			}
		}
		try {
			key.setOutFields(finalLocation);
			key.setSuffix("WAREHOUSE_TMP_LOCATION");
			context.write(key, NullWritable.get());
		} catch (Exception e) {
			System.out.println("finalLocation->" + finalLocation);
			e.printStackTrace();
		}
	}
}
