package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPay;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPayRolling;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHUidRollingMapper
 * 
 * @author sam.xie
 * @date 2015年7月20日 下午3:23:33
 * @version 1.0
 */
public class WHUidRollingReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private OnlineAndPay dayStat = new OnlineAndPay();// 天记录汇总
	private OnlineAndPayRolling rollingStat = new OnlineAndPayRolling();// 滚存记录
	private Map<Integer, OnlineAndPay> dayStatMap = new HashMap<Integer, OnlineAndPay>(); // 保存每天在线/付费记录
	private Date scheduleTime = null;
	private int statTime = 0;

	@Override
	protected void setup(Context context) {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		dayStat.clear();
		rollingStat.clear();
		dayStatMap.clear();
		int keyOffset = 1;
		if (Constants.SUFFIX_WAREHOUSE_UID_ROLLING.equals(key.getSuffix())) {
			keyOffset = 1;
		} else if (Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING.equals(key.getSuffix())) {
			keyOffset = 4;// 这里取uid, appId, platform, channel
		} else if (Constants.SUFFIX_WAREHOUSE_APP_ROLLING.equals(key.getSuffix())) {
			keyOffset = 3;
		} else {
			return;
		}
		if (Constants.SUFFIX_WAREHOUSE_UID_ROLLING.equals(key.getSuffix())
				|| Constants.SUFFIX_WAREHOUSE_APP_ROLLING.equals(key.getSuffix())) {
			for (OutFieldsBaseModel value : values) {
				String valSuffix = value.getSuffix();
				String[] valArr = value.getOutFields();
				if ("O".equals(valSuffix)) { // 在线
					int realDate = StringUtil.convertInt(valArr[0], 0);
					int loginTimes = StringUtil.convertInt(valArr[1], 0);
					int duration = StringUtil.convertInt(valArr[2], 0);
					dayStat.setLastUpdateTime(realDate);// 最后更新时间
					dayStat.setFirstLoginTime(realDate);
					dayStat.setLastLoginTime(realDate);
					dayStat.addLoginTimes(loginTimes);
					dayStat.addDuration(duration);

					// 保存每天的在线明细
					if (dayStatMap.get(realDate) == null) {
						dayStatMap.put(realDate, new OnlineAndPay());
					}
					dayStatMap.get(realDate).addLoginTimes(loginTimes);
					dayStatMap.get(realDate).addDuration(duration);
				} else if ("P".equals(valSuffix)) { // 付费
					int realDate = StringUtil.convertInt(valArr[0], 0);
					int payTimes = StringUtil.convertInt(valArr[1], 0);
					int payAmount = StringUtil.convertInt(valArr[2], 0);
					dayStat.setLastUpdateTime(realDate);// 最后更新时间
					dayStat.setFirstPayTime(realDate);
					dayStat.setLastPayTime(realDate);
					dayStat.addPayTimes(payTimes);
					dayStat.addPayAmount(payAmount);

					// 保存每天的付费明细
					if (dayStatMap.get(realDate) == null) {
						dayStatMap.put(realDate, new OnlineAndPay());
					}
					dayStatMap.get(realDate).addPayTimes(payTimes);
					dayStatMap.get(realDate).addPayAmount(payAmount);
				} else if ("R".equals(valSuffix)) { // 滚存
					if (0 == rollingStat.getLastUpdateTime()) { // 滚存对象首次使用需要初始化
						rollingStat.setParams(keyOffset, valArr);
					} else { // 合并多个时间【不重合】的滚存对象，一般用于滚存初始化
						rollingStat.mergeRolling(keyOffset, valArr);
					}
				}
			}

			// 合并天数据
			rollingStat.mergeDay(dayStat);

			// 更新天明细，天数
			mergeRecord(rollingStat, dayStatMap);

			valFields.setOutFields(rollingStat.toArray());
			context.write(key, valFields);
		} else if (Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING.equals(key.getSuffix())) {
			int firstUpdateTime = 0;
			String firstPlatform = "";
			String firstChannel = "";
			for (OutFieldsBaseModel value : values) {
				String valSuffix = value.getSuffix();
				String[] valArr = value.getOutFields();
				if ("O".equals(valSuffix)) { // 在线
					int realDate = StringUtil.convertInt(valArr[0], 0);
					int loginTimes = StringUtil.convertInt(valArr[1], 0);
					int duration = StringUtil.convertInt(valArr[2], 0);
					String platform = valArr[3];
					String channel = valArr[4];
					if (firstUpdateTime == 0 || firstUpdateTime > realDate) {
						firstUpdateTime = realDate;
						firstPlatform = platform;
						firstChannel = channel;
					}
					dayStat.setLastUpdateTime(realDate);// 最后更新时间
					dayStat.setFirstLoginTime(realDate);
					dayStat.setLastLoginTime(realDate);
					dayStat.addLoginTimes(loginTimes);
					dayStat.addDuration(duration);

					// 保存每天的在线明细
					if (dayStatMap.get(realDate) == null) {
						dayStatMap.put(realDate, new OnlineAndPay());
					}
					dayStatMap.get(realDate).addLoginTimes(loginTimes);
					dayStatMap.get(realDate).addDuration(duration);
				} else if ("P".equals(valSuffix)) { // 付费
					int realDate = StringUtil.convertInt(valArr[0], 0);
					int payTimes = StringUtil.convertInt(valArr[1], 0);
					int payAmount = StringUtil.convertInt(valArr[2], 0);
					String platform = valArr[3];
					String channel = valArr[4];
					if (firstUpdateTime == 0 || firstUpdateTime > realDate) {
						firstUpdateTime = realDate;
						firstPlatform = platform;
						firstChannel = channel;
					}
					dayStat.setLastUpdateTime(realDate);// 最后更新时间
					dayStat.setFirstPayTime(realDate);
					dayStat.setLastPayTime(realDate);
					dayStat.addPayTimes(payTimes);
					dayStat.addPayAmount(payAmount);

					// 保存每天的付费明细
					if (dayStatMap.get(realDate) == null) {
						dayStatMap.put(realDate, new OnlineAndPay());
					}
					dayStatMap.get(realDate).addPayTimes(payTimes);
					dayStatMap.get(realDate).addPayAmount(payAmount);
				} else if ("R".equals(valSuffix)) { // 滚存
					// 这里将platform ,channel统一设为"-"，避免出现一个uid对应多个平台，多个渠道的key，后面统一设置
					String platform = valArr[2];
					String channel = valArr[3];
					valArr[2] = "-";
					valArr[3] = "-";
					if (0 == rollingStat.getLastUpdateTime()) { // 滚存对象首次使用需要初始化
						rollingStat.setParams(keyOffset, valArr);
					} else { // 合并多个时间【不重合】的滚存对象，一般用于滚存初始化
						rollingStat.mergeRolling(keyOffset, valArr);

					}
					// 取时间最早的更新platform, channel
					if (firstUpdateTime == 0 || firstUpdateTime > rollingStat.getFirstLoginTime()
							|| firstUpdateTime > rollingStat.getFirstPayTime()) {
						firstUpdateTime = Math.min(rollingStat.getFirstLoginTime(), rollingStat.getFirstPayTime());
						firstPlatform = platform;
						firstChannel = channel;
					}
				}
			}

			// 合并天数据
			rollingStat.mergeDay(dayStat);

			// 更新天明细，天数
			mergeRecord(rollingStat, dayStatMap);

			// key中输出uid, appId, platform, channel
			key.setOutFields(new String[] { key.getOutFields()[0], key.getOutFields()[1], firstPlatform, firstChannel });

			valFields.setOutFields(rollingStat.toArray());
			context.write(key, valFields);
		}

	}

	private void mergeRecord(OnlineAndPayRolling rolling, Map<Integer, OnlineAndPay> map) {
		int loginMark = rolling.getLoginMark();
		int payMark = rolling.getPayMark();
		TreeMap<Integer, String> loginMap = str2MapWithDayLimit(rolling.getLoginRecord());
		TreeMap<Integer, String> payMap = str2MapWithDayLimit(rolling.getPayRecord());
		int dayOffset = 0;

		if (null != map && map.size() > 0) {
			for (Entry<Integer, OnlineAndPay> entry : map.entrySet()) {
				int realDate = entry.getKey();
				OnlineAndPay record = entry.getValue();
				dayOffset = (statTime - realDate) / WHConstants.SECONDS_IN_ONE_DAY;
				if (dayOffset >= 0 && dayOffset <= 29) { // 这里限定为最近30天
					if (record.getLoginTimes() > 0) { // 记录登录
						loginMark = loginMark | (1 << dayOffset);
						loginMap.put(realDate, realDate + ":" + record.getLoginTimes() + ":" + record.getDuration());
						rolling.addLoginDays(1);
					}
					if (record.getPayTimes() > 0) { // 记录付费
						payMark = payMark | (1 << dayOffset);
						payMap.put(realDate, realDate + ":" + record.getPayTimes() + ":" + record.getPayAmount());
						rolling.addPayDays(1);
					}
				}
			}
		}
		//
		rolling.setLoginMark(loginMark);
		rolling.setPayMark(payMark);
		rolling.setLoginRecord(map2Str(loginMap));
		rolling.setPayRecord(map2Str(payMap));
	}

	private TreeMap<Integer, String> str2MapWithDayLimit(String record) {
		TreeMap<Integer, String> map = new TreeMap<Integer, String>();
		if ((!StringUtil.isEmpty(record)) && (!"-".equals(record))) {
			String[] dayArr = record.split(",");
			for (String dayRecord : dayArr) {
				String[] items = dayRecord.split(":");
				int realDate = StringUtil.convertInt(items[0], statTime);
				int dayOffset = (statTime - realDate) / WHConstants.SECONDS_IN_ONE_DAY;
				if (dayOffset >= 0 && dayOffset <= 29) {
					// 统计日最近30天的数据算有效
					map.put(realDate, dayRecord);
				}
			}
		}
		return map;
	}

	private String map2Str(TreeMap<Integer, String> map) {
		StringBuilder sb = new StringBuilder("");
		if (null != map && map.size() > 0) {
			for (String val : map.values()) {
				sb.append(val + ",");
			}
		}
		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		}
		return "-";
	}
}
