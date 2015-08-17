package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHDataUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 数据仓库：付费按天汇总
 * 
 * @see WHPaymentDayMapper
 * 
 * 
 * @author sam.xie
 * @date 2015年6月27日 下午6:04:31
 * @version 1.0
 */
public class WHPaymentDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private Map<Integer, Integer> onlineMap = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> hourPayTimesMap = new HashMap<Integer, Integer>();
	private Map<Integer, Float> hourPayAmountMap = new HashMap<Integer, Float>();
	private Calendar cal = Calendar.getInstance();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		onlineMap.clear();
		hourPayTimesMap.clear();
		hourPayAmountMap.clear();

		String[] keyFields = key.getOutFields();
		String appId = keyFields[2];
		String dayOfWeek = keyFields[keyFields.length - 1];
		int totalPayTimes = 0; // 累计付费次数
		float totalPayAmount = 0;// 累计付费金额
		int maxLevel = 0; // 最大级别
		String[] lastValArray = null;
		for (OutFieldsBaseModel val : values) {
			lastValArray = val.getOutFields();
			int level = StringUtil.convertInt(lastValArray[WHConstants.IDX_LEVEL], 1);
			int payTime = StringUtil.convertInt(lastValArray[WHConstants.IDX_PAYMENT_PAYTIME], 0);
			float payAmount = StringUtil.convertFloat(lastValArray[WHConstants.IDX_PAYMENT_CURRENCYAMOUNT], 0);
			float payAmountAfterExchange = WHDataUtil.getExchangeAmount(payAmount, appId);
			totalPayTimes++;
			totalPayAmount += payAmountAfterExchange;
			if (maxLevel < level) {
				maxLevel = level;
			}
			setHourRecord(payTime, payAmountAfterExchange);
		}

		lastValArray[WHConstants.IDX_LEVEL] = maxLevel + "";
		lastValArray[WHConstants.IDX_PAYMENT_TOTALPAYTIMES] = totalPayTimes + "";
		lastValArray[WHConstants.IDX_PAYMENT_TOTALPAYAMOUNT] = StringUtil.getFloatString(totalPayAmount, 1);
		lastValArray[WHConstants.IDX_PAYMENT_CURRENCYTYPE] = "-";
		lastValArray[WHConstants.IDX_PAYMENT_PAYTYPE] = "-";
		lastValArray[WHConstants.IDX_PAYMENT_HOURRECORD] = getHourRecord(hourPayTimesMap, hourPayAmountMap);
		lastValArray[WHConstants.IDX_PAYMENT_DAYOFWEEK] = dayOfWeek;
		key.setOutFields(lastValArray);
		context.write(key, NullWritable.get());
	}

	private void setHourRecord(int payTime, float payAmount) {
		cal.setTimeInMillis(payTime * 1000L);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		// 当前小时
		int currentHour = cal.get(Calendar.HOUR_OF_DAY);

		Integer hourPayTimes = hourPayTimesMap.get(currentHour);
		if (null == hourPayTimes) {
			hourPayTimes = 0;
		}
		hourPayTimesMap.put(currentHour, hourPayTimes + 1);

		Float hourPayAmount = hourPayAmountMap.get(currentHour);
		if (null == hourPayAmount) {
			hourPayAmount = 0f;
		}
		hourPayAmountMap.put(currentHour, hourPayAmount + payAmount);
	}

	private String getHourRecord(Map<Integer, Integer> hourTimesMap, Map<Integer, Float> hourDurationMap) {
		StringBuilder sb = new StringBuilder("");
		for (int hour = 0; hour < 24; hour++) {
			if (null != hourPayTimesMap.get(hour) || null != hourPayAmountMap.get(hour)) {
				int payTimes = hourPayTimesMap.get(hour) == null ? 0 : hourPayTimesMap.get(hour);
				float payAmount = hourDurationMap.get(hour) == null ? 0f : hourPayAmountMap.get(hour);
				String hourStr = hour < 10 ? "0" + hour : "" + hour;
				sb.append(hourStr + ":" + payTimes + ":" + StringUtil.getFloatString(payAmount, 1) + ","); // 保留小数
			}
		}
		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		}
		return "-";
	}
}
