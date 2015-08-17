package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 计算每个小时 用户的流量情况
 * 依赖上个小时的（后缀为 _DESelf_APP_TRAFFIC）
 * 
 * key:   AppID,Platform, Channel, GAMESERVER,UP|DOWN,NetType 
 * value: Value
 * 
 * 
 * @author Ivan <br>
 * @date 2014-6-6 <br>
 * @version 1.0
 * <br>
 */
public class NetworkTrafficHourReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, LongWritable> {
	private LongWritable outputValue = new LongWritable();
	private BloomFilter<String> bloomFilter = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 1000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.000001);
		bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> value, Context context)
			throws IOException, InterruptedException {
		// reset bloomfilter
		bloomFilter.clear();

		// 把每一个value 加和就行了
		long sum = 0;
		long accountIDSize = 0;
		for (OutFieldsBaseModel outFields : value) {
			String[] values = outFields.getOutFields();
			String uid = values[0];
			if (!bloomFilter.contains(uid)) {
				bloomFilter.add(uid);
				accountIDSize++;
			}
			String traffic = values[1];
			sum += StringUtil.convertInt(traffic, 0);
		}
		// 求均值
		// if (accountIDSize > 0 && sum > 0) {
		if (sum > 0) {
			outputValue.set(sum);
			key.setSuffix(Constants.SUFFIX_TRAFFIC_HOUR);
			context.write(key, outputValue);
		}
	}
}
