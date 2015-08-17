package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 对于 RINGTONE 直接对设备去重
 * 对于APP 求同一个设备的最大 启动次数、最大在线时间
 * @author Ivan     <br>
 * @date 2014-6-9         <br>
 * @version 1.0
 * <br>
 */

public class AppListReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private BloomFilter<String> bloomFilter = null;
	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 1000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.000001);
		bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		bloomFilter.clear();
		String suffix = key.getSuffix();
		if (Constants.SUFFIX_APPLIST_RINGTONE.equals(suffix)) {
			int uidSum = 0;
			for (OutFieldsBaseModel value : values) {
				String uid = value.getOutFields()[0];
				if (!bloomFilter.contains(uid)) {
					bloomFilter.add(uid);
					uidSum++;
				}
			}
			outputValue.setOutFields(new String[] { uidSum + "" });
			context.write(key, outputValue);
		} else if (Constants.SUFFIX_APPLIST_APP.equals(suffix)) {
			// { appId, platForm, appName, appVer, uid };
			// { setupTimes, upTime };
			int maxSetupTimes = 0;
			int maxUpTime = 0;
			for (OutFieldsBaseModel value : values) {
				int currentSetupTimes = StringUtil.convertInt(value.getOutFields()[0], 0);
				int currentUpTime = StringUtil.convertInt(value.getOutFields()[1], 0);
				maxSetupTimes = maxSetupTimes > currentSetupTimes ? maxSetupTimes : currentSetupTimes;
				maxUpTime = maxUpTime > currentUpTime ? maxUpTime : currentUpTime;
			}
			outputValue.setOutFields(new String[] { maxSetupTimes + "", maxUpTime + "" });
			context.write(key, outputValue);
		}
	}
}
