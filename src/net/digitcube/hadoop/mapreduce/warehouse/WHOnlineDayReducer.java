package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHOnlineDayMapper
 * 
 * @author sam.xie
 * @date 2015年6月26日 下午3:23:33
 * @version 1.0
 */
public class WHOnlineDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private Map<Integer, Integer> onlineMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> hourLoginTimesMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> hourDurationMap = new HashMap<Integer, Integer>();
	private Calendar cal = Calendar.getInstance();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		onlineMap.clear();
		hourLoginTimesMap.clear();
		hourDurationMap.clear();

		String[] keyFields = key.getOutFields();
		String dayOfWeek = keyFields[keyFields.length - 1];
		int totalLoginTimes = 0; // 累计登录次数
		int totalDuration = 0;// 累计在线时长
		int maxLevel = 0; // 最大级别
		String[] lastValArray = null;
		for (OutFieldsBaseModel val : values) {
			lastValArray = val.getOutFields();
			int level = StringUtil.convertInt(lastValArray[WHConstants.IDX_LEVEL], 1);
			int loginTime = StringUtil.convertInt(lastValArray[WHConstants.IDX_ONLINE_LOGINTIME], 0);
			int duration = StringUtil.convertInt(lastValArray[WHConstants.IDX_ONLINE_DURATION], 0);
			totalDuration += duration;
			totalLoginTimes++;
			if (maxLevel < level) {
				maxLevel = level;
			}
			setHourRecord(loginTime, loginTime + duration);
		}

		lastValArray[WHConstants.IDX_LEVEL] = maxLevel + "";
		lastValArray[WHConstants.IDX_ONLINE_TOTALLOGINTIMES] = totalLoginTimes + "";
		lastValArray[WHConstants.IDX_ONLINE_TOTALDURATION] = totalDuration + "";
		lastValArray[WHConstants.IDX_ONLINE_DAYOFWEEK] = dayOfWeek;
		lastValArray[WHConstants.IDX_ONLINE_HOURRECORD] = getHourRecord(hourLoginTimesMap, hourDurationMap);
		key.setOutFields(lastValArray);
		context.write(key, NullWritable.get());
	}

	// 把登录时间和在线时长转换为每小时的登录次数在线时长
	/**
	 * <pre>
	 * 	  -------duration------
	 *    |login		logout|
	 * 0------1------2------3------4------5
	 */
	private void setHourRecord(int loginTime, int logoutTime) {
		int currentTime = loginTime;
		cal.setTimeInMillis(loginTime * 1000L);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		// 当前小时
		int currentHour = cal.get(Calendar.HOUR_OF_DAY);
		// 下一小时
		cal.add(Calendar.HOUR_OF_DAY, 1);
		int nextHourTime = (int) (cal.getTimeInMillis() / 1000);

		Integer hourTimes = hourLoginTimesMap.get(currentHour);
		if (null == hourTimes) {
			hourTimes = 0;
		}
		hourLoginTimesMap.put(currentHour, hourTimes + 1);

		Integer hourDuration = 0;
		int duration = 0;
		while (nextHourTime < logoutTime) {
			hourDuration = hourDurationMap.get(currentHour);
			if (null == hourDuration) {
				hourDuration = 0;
			}
			duration = nextHourTime - currentTime;
			hourDurationMap.put(currentHour, hourDuration + duration);
			currentTime = nextHourTime;
			currentHour = cal.get(Calendar.HOUR_OF_DAY);
			cal.add(Calendar.HOUR_OF_DAY, 1);
			nextHourTime = (int) (cal.getTimeInMillis() / 1000);
		}
		hourDuration = hourDurationMap.get(currentHour);
		if (null == hourDuration) {
			hourDuration = 0;
		}
		hourDurationMap.put(currentHour, hourDuration + (logoutTime - currentTime));
	}

	private String getHourRecord(Map<Integer, Integer> hourTimesMap, Map<Integer, Integer> hourDurationMap) {
		StringBuilder sb = new StringBuilder("");
		for (int hour = 0; hour < 24; hour++) {
			if (null != hourTimesMap.get(hour) || null != hourDurationMap.get(hour)) {
				int loginTimes = hourTimesMap.get(hour) == null ? 0 : hourTimesMap.get(hour);
				int duration = hourDurationMap.get(hour) == null ? 0 : hourDurationMap.get(hour);
				String hourStr = hour < 10 ? "0" + hour : "" + hour;
				sb.append(hourStr + ":" + loginTimes + ":" + duration + ",");
			}
		}
		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		}
		return "-";
	}
}
