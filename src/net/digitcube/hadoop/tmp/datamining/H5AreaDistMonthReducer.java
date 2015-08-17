package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class H5AreaDistMonthReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, IntWritable> {

	IntWritable valObj = new IntWritable();
	BloomFilter<String> bloom = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 100000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.0000001);
		bloom = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		bloom.clear();
		int total = 0;
		for (OutFieldsBaseModel val : values) {
			String accountId = val.getOutFields()[0];
			if (!bloom.contains(accountId)) {
				// UID 不存在，把 UID 添加到集合同时 UID 计数加 1
				bloom.add(accountId);
				total++;
			}
		}
		valObj.set(total);
		context.write(key, valObj);
	}
}
