package net.digitcube.hadoop.mapreduce.oss;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月22日 下午8:31:00 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class OSSReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		int totalCost = 0;
		int totalSuccess = 0;
		int totalFail = 0;
		for (OutFieldsBaseModel val : values) {
			String[] valArray = val.getOutFields();
			totalCost += StringUtil.convertInt(valArray[0], 0);
			totalSuccess += StringUtil.convertInt(valArray[1], 0);
			totalFail += StringUtil.convertInt(valArray[2], 0);
		}

		key.setSuffix(Constants.SUFFIX_OSS);
		float avgCost = totalCost / (totalSuccess + totalFail);
		context.write(key, new OutFieldsBaseModel(
				new String[] { totalCost + "", totalSuccess + "",
						totalFail + "", avgCost + "" }));

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
