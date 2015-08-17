package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：付费表
 * 
 * 输入：@see WHOnlineRollingMapper
 * 设备表（滚存），ios设备的imei取的是idfa
 * uid	mac	imei	platform	brand	resolution	createTime 
 * 
 * 输出：指定mac的设备信息
 * channel	uid	mac	imei	platform	brand	resolution	createTime
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午4:39:22
 * @version 1.0
 */
public class TmpWHMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	public Map<String, String> macMap = new HashMap<String, String>();

	protected void setup(Context context) throws IOException, InterruptedException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				TmpWHMapper.class.getResourceAsStream("mac.list")));
		String line = null;
		while (null != (line = reader.readLine())) {
			System.out.println(line);
			String channel = line.split("\t")[0];
			String mac = line.split("\t")[1];
			macMap.put(mac, channel);
		}
		System.out.println(">>>>>>>>>:" + macMap.size());

	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String mac = paramsArr[1];
		// 数据过滤
		if (macMap.keySet().contains(mac)) {
			keyFields.setSuffix("WAREHOUSE-TMP");
			keyFields.setOutFields(new String[] { macMap.get(mac) }); // 渠道作为key
			valFields.setOutFields(paramsArr);
			context.write(keyFields, valFields);
		}
	}
}
