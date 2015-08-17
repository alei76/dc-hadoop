package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.IP;
import net.digitcube.hadoop.model.warehouse.WHOnlineLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：在线表统计
 * 
 * 输入：
 * 1.在线明细：@see WHOnlineDetailMapper
 * uid, accountId, appId, platform, channel, level, ts, ip, country, province, city, cleanFlag, 
 * loginTime, duration, brand, resolution, os, mac, imei, imsi, idfa, operator, netType
 * 
 * 输出：
 * 1.uid地区活跃表	@see {@link Constants#SUFFIX_WAREHOUSE_UID_LOCATION}
 * uid
 * country	province	city	area	ip	netType
 * country_07_09	province_07_09	city_07_09	area_07_09	ip_07_09	netType_07_09
 * country_09_18	province_09_18	city_09_18	area_09_18	ip_09_18	netType_09_18
 * country_18_24	province_18_24	city_18_24	area_18_24	ip_18_24	netType_18_24
 * 
 * @author sam.xie
 * @date 2015年7月13日 下午19:20:33
 * @version 1.0
 */
public class WHUidLocationDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	@Override
	protected void setup(Context context) {
		IP.init();
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		WHOnlineLog log = null;
		try {
			log = new WHOnlineLog(paramsArr);
		} catch (Exception e) {
			return;
		}
		if (!"-".equals(log.getCleanFlag()) && !"2.1".equals(log.getCleanFlag())) {
			return;
		}

		String uid = log.getUid();
		int loginTime = log.getLoginTime();
		String ip = log.getIp();
		String country = log.getCountry();
		String province = log.getProvince();
		String city = log.getCity();
		String area = "-";
		// String operator = "-";
		int netType = StringUtil.convertInt(log.getNetType(), 3); // 3表示OTHERS网络，@see NetType

		String[] locationFromIP = new String[] { "-", "-", "-", "-", "-" };
		if ((!StringUtil.isEmpty(ip)) && (!"-".equals(ip))) {
			try {
				locationFromIP = IP.find(ip);
				country = StringUtil.isEmpty(locationFromIP[0]) ? "-" : locationFromIP[0];
				province = StringUtil.isEmpty(locationFromIP[1]) ? "-" : locationFromIP[1];
				city = StringUtil.isEmpty(locationFromIP[2]) ? "-" : locationFromIP[2];
				area = StringUtil.isEmpty(locationFromIP[3]) ? "-" : locationFromIP[3];
				// operator = StringUtil.isEmpty(locationFromIP[4]) ? "-" : locationFromIP[4];
			} catch (Exception e) {
			}
		}

		// UID地区活跃表
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_LOCATION);
		keyFields.setOutFields(new String[] { uid });
		valFields.setOutFields(new String[] { loginTime + "", country, province, city, area, ip, netType + "" });
		context.write(keyFields, valFields);
	}
}
