package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * <pre>
 * 虚拟币收入、支出、收入方式分布、支出方式分布 Reducer
 * Title: CoinMapper.java<br>
 * Description: CoinMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-11-6         <br>
 * @version 1.0
 * <br>
 * 
 * 其依赖的Map 输出：
 * [ appID, platform, channel, gameServer, roomid, id ,Flag ][num ]
 * [ appID, platform, channel, gameServer, id ,Flag ][ num ]
 * 
 * Reduce 输出
 * [ appID, platform, channel, gameServer, roomid, id ,Flag ,sum(num)]
 * [ appID, platform, channel, gameServer, id ,Flag , sum(num)]
 */
public class CoinLayoutReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, LongWritable> {
	// 统计虚拟币消耗方式人数分布
	private BloomFilter<String> bloom = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 10000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.0000001);
		//expectedNumberOfElements=1kw, falsePositiveProbability=0.0000001 占不到 50MB 内存
		bloom = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		if(Constants.SUFFIX_COIN_LOST_PLAYER_NUM.equals(key.getSuffix())){
			long playerCount = 0;
			bloom.clear();
			for (Text val : values) {
				String accountId = val.toString();
				if(!bloom.contains(accountId)){
					// accountId 不存在，把 accountId 添加到集合同时计数加 1 
					bloom.add(accountId);
					playerCount++;
				}
			}
			context.write(key, new LongWritable(playerCount));
			
		} else {
			long sum = 0;
			for (Text val : values) {
				sum += StringUtil.convertLong(val.toString(), 0);
			}
			context.write(key, new LongWritable(sum));
		}
	}
}
