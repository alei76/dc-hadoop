package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：付费表
 * 
 * 输入：
 * 
 * 1.UID 30天的活跃地区		@see WHUidLocationDayMapper
 * uid
 * country	province	city	area	ip	netType
 * province_07_09	city_07_09	area_07_09	ip_07_09	netType_07_09
 * province_09_18	city_09_18	area_09_18	ip_09_18	netType_09_18
 * province_18_24	city_18_24	area_18_24	ip_18_24	netType_18_24
 * 
 * -- 2.UID+APP 30天小时统计	@see WHUidHourMapper
 * -- uid	appId
 * -- weekType dim	total_value 00_value 01_value ... 23_value	// 24小时指标统计
 * 
 * update 
 * 2.UID + APP 日统计 @see WHOnlineDayMapper
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * totalLoginTimes	totalDuration	brand	resolution	os	mac	imei	imsi	idfa	[hour:times:duration,...,hour:times:duration]	dayOfWeek
 * 
 * 
 * 输出：
 * 1.汇总统计
 * uid	appid	loginTimes	duration	payTimes	payAmount
 * 
 * 2.小时分布
 * uid	appid	00_duration	01_duration	...	23_duration
 * 
 * 3.地区分布
 * uid	province	loginTimes	duration	payTimes	payAmount
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午4:39:22
 * @version 1.0
 */
public class Tmp2WHMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Set<String> uidSet = new HashSet<String>();
	private String fileSuffix = "";

	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				TmpWHMapper.class.getResourceAsStream("uid.list")));
		String line = null;
		while (null != (line = reader.readLine())) {
			System.out.println(line);
			String uid = line.split("\t")[0];
			uidSet.add(uid);
		}
		System.out.println(">>>>>>>>>:" + uidSet.size());
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_LOCATION)) {
			String uid = paramsArr[0];
			if (!uidSet.contains(uid)) {
				return;
			}
			keyFields.setOutFields(new String[] { uid });
			valFields.setOutFields(paramsArr);
			valFields.setSuffix("L");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UIDAPP_HOUR)) {
			String uid = paramsArr[0];
			String weekType = paramsArr[2];
			if (!uidSet.contains(uid)) {
				return;
			}
			if (!"1".equals(weekType)) {
				return;
			}
			keyFields.setOutFields(new String[] { uid });
			valFields.setOutFields(paramsArr);
			valFields.setSuffix("H");
			context.write(keyFields, valFields);
		}
	}
}
