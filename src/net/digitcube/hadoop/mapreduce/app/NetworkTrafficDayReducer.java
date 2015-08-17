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
 * 	StatiTime bigint not null default 0 comment '统计日期,1970年以来的秒数',
 * 	PlatFormType tinyint not null default 0 comment '平台类型1-ios,2-android,3-windows', 
 * 	ChannelID int not null default 0 comment '渠道ID',
 * 	GameRegionID int not null default 0 comment '游戏区服ID',
 * 	AppVersion int not null default 0 comment '游戏版本',
 * 	Type varchar(10) not null default '' comment '上行UP下行DOWN',
 * 	NetType varchar(20) not null default '' comment 'wifi 2G 3G _ALL_NT',
 * 	Value int not null default 0 comment '多少流量',
 * 	PlayerNum int not null default 0 comment '多少人',
 * Title: NetworkTrafficDayReducer.java<br>
 * Description: NetworkTrafficDayReducer.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2014-6-6         <br>
 * @version 1.0
 * <br>
 */
public class NetworkTrafficDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private BloomFilter<String> bloomFilter = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 1000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.000001);
		bloomFilter = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}

	private OutFieldsBaseModel outputValue = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		// 求流量 求人数
		outputValue.reset();
		bloomFilter.clear();
		long trafficSum = 0;
		long uidSum = 0;
		for (OutFieldsBaseModel value : values) {
			String[] valueArr = value.getOutFields();
			String uid = valueArr[0];
			if (!bloomFilter.contains(uid)) {
				bloomFilter.add(uid);
				uidSum++;
			}
			String traffic = valueArr[1];
			int trafficInt = StringUtil.convertInt(traffic, 0);
			trafficSum += trafficInt;
		}
		key.setSuffix(Constants.SUFFIX_TRAFFIC_DAY);
		outputValue.setOutFields(new String[] { trafficSum + "", uidSum + "" });
		context.write(key, outputValue);
	}
}
