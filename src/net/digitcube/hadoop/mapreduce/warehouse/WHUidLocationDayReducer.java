package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHUidLocationDayMapper
 * 
 * @author sam.xie
 * @date 2015年7月13日 下午19:23:33
 * @version 1.0
 */
public class WHUidLocationDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Calendar cal = Calendar.getInstance();
	private final static int WIFI = 2;

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		// 分别取一天中最后一次，7~9点，9~18点，18~24点最后一次上报在线的记录
		int lastLoginTime = 0;
		int lastLoginTime_07_09 = 0;
		int lastLoginTime_09_18 = 0;
		int lastLoginTime_18_24 = 0;

		// country, province, city, area, ip, netType, extend
		String[] lastRecord = { "-", "-", "-", "-", "-", "-", "-" };
		String[] lastRecord_07_09 = { "-", "-", "-", "-", "-", "-" };
		String[] lastRecord_09_18 = { "-", "-", "-", "-", "-", "-" };
		String[] lastRecord_18_24 = { "-", "-", "-", "-", "-", "-" };

		// 是否存在wifi上报，如果存在就以最后一次wifi作为结果
		boolean hasWifi = false;
		boolean hasWifi_07_09 = false;
		boolean hasWifi_09_18 = false;
		boolean hasWifi_18_24 = false;

		for (OutFieldsBaseModel val : values) {
			String[] fields = val.getOutFields();
			int loginTime = Integer.parseInt(fields[0]);
			String country = fields[1];
			String province = fields[2];
			String city = fields[3];
			String area = fields[4];
			String ip = fields[5];
			int netType = Integer.parseInt(fields[6]);
			// 取最后一次上报的wifi网络地区信息，如果不存在wifi网络，就取最后一次上报的网络地区信息
			if (lastLoginTime < loginTime) {
				if (netType == WIFI) { // 记录是否存在过wifi
					hasWifi = true;
				}
				if ((hasWifi && netType == WIFI) || (!hasWifi)) {
					lastLoginTime = loginTime;
					lastRecord = new String[] { country, province, city, area, ip, netType + "" };
				}
			}
			if (checkInterval(loginTime, 7, 9) && lastLoginTime_07_09 < loginTime) {
				if (netType == WIFI) { // 记录是否存在过wifi
					hasWifi_07_09 = true;
				}
				if ((hasWifi_07_09 && netType == WIFI) || (!hasWifi_07_09)) {
					lastLoginTime_07_09 = loginTime;
					lastRecord_07_09 = new String[] { country, province, city, area, ip, netType + "" };
				}
			}
			if (checkInterval(loginTime, 9, 18) && lastLoginTime_09_18 < loginTime) {
				if (netType == WIFI) { // 记录是否存在过wifi
					hasWifi_09_18 = true;
				}
				if ((hasWifi_09_18 && netType == WIFI) || (!hasWifi_09_18)) {
					lastLoginTime_09_18 = loginTime;
					lastRecord_09_18 = new String[] { country, province, city, area, ip, netType + "" };
				}
			}
			if (checkInterval(loginTime, 18, 24) && lastLoginTime_18_24 < loginTime) {
				if (netType == WIFI) { // 记录是否存在过wifi
					hasWifi_18_24 = true;
				}
				if ((hasWifi_18_24 && netType == WIFI) || (!hasWifi_18_24)) {
					lastLoginTime_18_24 = loginTime;
					lastRecord_18_24 = new String[] { country, province, city, area, ip, netType + "" };
				}
			}
		}

		// 合并各时段的结果
		String[] result = mergeArray(lastRecord, lastRecord_07_09, lastRecord_09_18, lastRecord_18_24);
		valFields.setOutFields(result);
		context.write(key, valFields);
	}

	private boolean checkInterval(long loginTime, int startHour, int endHour) {
		cal.setTimeInMillis(loginTime * 1000);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		if (hour >= startHour && hour < endHour) {
			return true;
		} else {
			return false;
		}
	}

	private String[] mergeArray(String[]... arrays) {
		int totalLength = 0;
		for (String[] array : arrays) {
			totalLength += array.length;
		}
		String[] mergedArray = new String[totalLength];
		int i = 0;
		for (String[] array : arrays) {
			for (String item : array) {
				mergedArray[i++] = item;
			}
		}
		return mergedArray;
	}

}
