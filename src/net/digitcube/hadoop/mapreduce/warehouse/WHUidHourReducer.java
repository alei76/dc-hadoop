package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPay;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 输入
 * @see WHUidHourMapper
 * 
 * @author sam.xie
 * @date 2015年7月25日 下午2:23:33
 * @version 1.0
 */
public class WHUidHourReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFileds = new OutFieldsBaseModel();
	// 保存每小时的统计
	private Map<String, OnlineAndPay> mapAll = new TreeMap<String, OnlineAndPay>();
	private Map<String, OnlineAndPay> mapWeekday = new TreeMap<String, OnlineAndPay>();
	private Map<String, OnlineAndPay> mapWeekend = new TreeMap<String, OnlineAndPay>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		mapAll.clear();
		mapWeekday.clear();
		mapWeekend.clear();
		int loginMark = 0;
		int payMark = 0;
		for (OutFieldsBaseModel val : values) {
			if ("O".equals(val.getSuffix())) {
				int intervalDays = StringUtil.convertInt(val.getOutFields()[0], 0); // 与统计日期的天间隔，0~29
				int dayOfWeek = StringUtil.convertInt(val.getOutFields()[1], 0); // 当前是星期几
				String hourRecord = val.getOutFields()[2];

				// 登录标记
				loginMark = loginMark | (1 << intervalDays);

				String[] hourArr = hourRecord.split(",");
				// 计算小时分布
				for (String items : hourArr) {
					String[] item = items.split(":");
					String hour = item[0];
					int loginTimes = StringUtil.convertInt(item[1], 0);
					int duration = StringUtil.convertInt(item[2], 0);
					OnlineAndPay sumAll = mapAll.get(hour) == null ? new OnlineAndPay() : mapAll.get(hour);
					mapAll.put(hour, sumAll);
					sumAll.addLoginTimes(loginTimes);
					sumAll.addDuration(duration);
					if (dayOfWeek >= 2 && dayOfWeek <= 6) { // 工作日
						OnlineAndPay sumWeekday = mapWeekday.get(hour) == null ? new OnlineAndPay() : mapWeekday
								.get(hour);
						mapWeekday.put(hour, sumWeekday);
						sumWeekday.addLoginTimes(loginTimes);
						sumWeekday.addDuration(duration);
					} else { // 周末
						OnlineAndPay sumWeekend = mapWeekend.get(hour) == null ? new OnlineAndPay() : mapWeekend
								.get(hour);
						mapWeekend.put(hour, sumWeekend);
						sumWeekend.addLoginTimes(loginTimes);
						sumWeekend.addDuration(duration);
					}
				}
			} else if ("P".equals(val.getSuffix())) {
				int intervalDays = StringUtil.convertInt(val.getOutFields()[0], 0); // 与统计日期的天间隔，0~29
				int dayOfWeek = StringUtil.convertInt(val.getOutFields()[1], 0); // 当前是星期几
				String hourRecord = val.getOutFields()[2];

				// 付费标记
				payMark = payMark | (1 << intervalDays);

				String[] hourArr = hourRecord.split(",");
				// 计算小时分布
				for (String items : hourArr) {
					String[] item = items.split(":");
					String hour = item[0];
					int payTimes = StringUtil.convertInt(item[1], 0);
					float payAmount = StringUtil.convertFloat(item[2], 0);
					OnlineAndPay sumAll = mapAll.get(hour) == null ? new OnlineAndPay() : mapAll.get(hour);
					mapAll.put(hour, sumAll);
					sumAll.addPayTimes(payTimes);
					sumAll.addPayAmount(payAmount);
					if (dayOfWeek >= 2 && dayOfWeek <= 6) { // 工作日
						OnlineAndPay sumWeekday = mapWeekday.get(hour) == null ? new OnlineAndPay() : mapWeekday
								.get(hour);
						mapWeekday.put(hour, sumWeekday);
						sumWeekday.addPayTimes(payTimes);
						sumWeekday.addPayAmount(payAmount);
					} else { // 周末
						OnlineAndPay sumWeekend = mapWeekend.get(hour) == null ? new OnlineAndPay() : mapWeekend
								.get(hour);
						mapWeekend.put(hour, sumWeekend);
						sumWeekend.addPayTimes(payTimes);
						sumWeekend.addPayAmount(payAmount);
					}
				}
			}
		}
		// 只有统计日当天有登录或者付费才统计
		if ((loginMark & 1) != 1 && (payMark & 1) != 1) {
			return;
		}

		if (mapAll.size() > 0) {
			if (loginMark != 0) {
				valFileds.setOutFields(convertMap2String(mapAll, WHConstants.TYPE_ALL, WHConstants.DIM_LOGIN_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapAll, WHConstants.TYPE_ALL, WHConstants.DIM_DURATION));
				context.write(key, valFileds);
			}
			if (payMark != 0) {
				valFileds.setOutFields(convertMap2String(mapAll, WHConstants.TYPE_ALL, WHConstants.DIM_PAY_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapAll, WHConstants.TYPE_ALL, WHConstants.DIM_PAY_AMOUNT));
				context.write(key, valFileds);
			}
		}

		if (mapWeekday.size() > 0) {
			if (loginMark != 0) {
				valFileds.setOutFields(convertMap2String(mapWeekday, WHConstants.TYPE_WEEKDAY, WHConstants.DIM_LOGIN_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapWeekday, WHConstants.TYPE_WEEKDAY, WHConstants.DIM_DURATION));
				context.write(key, valFileds);
			}
			if (payMark != 0) {
				valFileds.setOutFields(convertMap2String(mapWeekday, WHConstants.TYPE_WEEKDAY, WHConstants.DIM_PAY_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapWeekday, WHConstants.TYPE_WEEKDAY, WHConstants.DIM_PAY_AMOUNT));
				context.write(key, valFileds);
			}
		}

		if (mapWeekend.size() > 0) {
			if (loginMark != 0) {
				valFileds.setOutFields(convertMap2String(mapWeekend, WHConstants.TYPE_WEEKEND, WHConstants.DIM_LOGIN_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapWeekend, WHConstants.TYPE_WEEKEND, WHConstants.DIM_DURATION));
				context.write(key, valFileds);
			}
			if (payMark != 0) {
				valFileds.setOutFields(convertMap2String(mapWeekend, WHConstants.TYPE_WEEKEND, WHConstants.DIM_PAY_TIMES));
				context.write(key, valFileds);
				valFileds.setOutFields(convertMap2String(mapWeekend, WHConstants.TYPE_WEEKEND, WHConstants.DIM_PAY_AMOUNT));
				context.write(key, valFileds);
			}
		}
	}

	/**
	 * <pre/>
	 * weekType 1.所有，2.工作日，3.周末
	 * dim 		1:登录次数，2:在线时长，3:付费次数，4:付费金额
	 */
	public String[] convertMap2String(Map<String, OnlineAndPay> map, int weekType, int dim) {
		String[] resultArr = new String[27];
		resultArr[0] = weekType + "";
		resultArr[1] = dim + "";
		resultArr[2] = "0";
		long totalValue = 0;
		double totalValueFloat = 0;
		OnlineAndPay item;
		for (int i = 0; i < 24; i++) {
			String hour = i < 10 ? "0" + i : "" + i;
			item = map.get(hour);
			if (item == null) {
				resultArr[i + 3] = "0";
			} else {
				if (dim == WHConstants.DIM_LOGIN_TIMES) {
					totalValue += item.getLoginTimes();
					resultArr[2] = totalValue + "";
					resultArr[i + 3] = item.getLoginTimes() + "";
				} else if (dim == WHConstants.DIM_DURATION) {
					totalValue += item.getDuration();
					resultArr[2] = totalValue + "";
					resultArr[i + 3] = item.getDuration() + "";
				} else if (dim == WHConstants.DIM_PAY_TIMES) {
					totalValue += item.getPayTimes();
					resultArr[2] = totalValue + "";
					resultArr[i + 3] = item.getPayTimes() + "";
				} else if (dim == WHConstants.DIM_PAY_AMOUNT) {
					totalValueFloat += item.getPayAmount();
					resultArr[2] = totalValueFloat + "";
					resultArr[i + 3] = item.getPayAmount() + "";
				}
			}
		}
		return resultArr;
	}
}
