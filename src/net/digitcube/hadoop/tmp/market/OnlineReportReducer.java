package net.digitcube.hadoop.tmp.market;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class OnlineReportReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private BloomFilter<String> bloom = null;
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	Set<String> accSet = new HashSet<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/*Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 100000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.00000001);
		//expectedNumberOfElements=1亿, falsePositiveProbability=0.00000001 占约 250MB 内存
		bloom = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);*/
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {

		//bloom.clear();
		accSet.clear();
		
		long totalSum = 0;
		for(OutFieldsBaseModel val : values){
			String accId = val.getOutFields()[0];
			accSet.add(accId);
			if(val.getOutFields().length > 1){
				totalSum += StringUtil.convertInt(val.getOutFields()[1], 0);
			}
		}
		
		int totalPlayerCount = accSet.size();
		long avgSum = totalSum / totalPlayerCount;
		
		String[] valFields = new String[]{
				totalPlayerCount + "",
				totalSum + "",
				avgSum + "",
		};
		valObj.setOutFields(valFields);
		context.write(key, valObj);
	}
}
