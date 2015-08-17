package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPayRolling;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：UID在线付费滚存统计
 * 
 * update by sam 2015-08-04 15:03:24
 * UID+APPID滚存修改，加入platform, channel, 导致字段解析修改
 * 
 * 
 * 输入：
 * 1.UID Tracking（滚存）	@see WHUidRollingMapper{@link Constants#SUFFIX_WAREHOUSE_UID_ROLLING}}
 * uid
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * 2.UID+APPID Tracking（滚存）	@see WHUidRollingMapper{@link Constants#SUFFIX_WAREHOUSE_UIDAPP_ROLLING}}
 * uid	appId	platform	channel
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * 3.UID （小时段统计）	@see WHUidHourMapper{@link Constants#SUFFIX_WAREHOUSE_UID_HOUR}
 * uid
 * weekType dim	total_value 00_value 01_value ... 23_value	// 24小时指标统计
 * 
 * 4.UID+APP 小时分布	@see WHUidHourMapper{@link Constants#SUFFIX_WAREHOUSE_UIDAPP_HOUR}
 * uid	appId
 * weekType dim	total_value 00_value 01_value ... 23_value	// 24小时指标统计
 * 
 * 5.UID 使用偏好 	@see WHUidAppHabitDayMapper
 * uid	appType	lastUpdateTime	firstLoginTime	lastLoginTime	loginTimes	duration	loginDays	firstPayTime	lastPayTime	payTimes	payAmount	payDays
 * 
 * 
 * 输出：
 * 1.UID 统计
 * uid
 * validDays	loginTimes	duration	payTimes	payAmount	// 历史统计
 * validDays_d30	loginTimes_d30	duration_d30	payTimes_d30	payAmount_d30	// 最近30天统计
 * validDays_d07	loginTimes_d07	duration_d07	payTimes_d07	payAmount_d07	// 最近7天统计
 * duration_d30_h00	duration_d30_h01	...	duration_d30_h22	duration_d30_h23	// 24小时在线时长（最近30天）
 * payTimes_d30_h00	payTimes_d30_h01	...	payTimes_d30_h22	payTimes_d30_h23	// 24小时付费次数（最近30天）
 * duration_d30_weekday_h00	duration_d30_weekday_h01	...	duration_d30_weekday_h22	duration_d30_weekday_h23	// 24小时在线时长（最近30天工作日）
 * payTimes_d30_weekday_h00	payTimes_d30_weekday_h01	...	payTimes_d30_weekday_h22	payTimes_d30_weekday_h23	// 24小时付费次数（最近30天工作日）
 * duration_d30_weekend_h00	duration_d30_weekend_h01	...	duration_d30_weekend_h22	duration_d30_weekend_h23	// 24小时在线时长（最近30天周末）
 * payTimes_d30_weekend_h00	payTimes_d30_weekend_h01	...	payTimes_d30_weekend_h22	payTimes_d30_weekend_h23	// 24小时付费次数（最近30天周末）
 * appType_d30_t1	validDays_d30_t1	loginTimes_d30_t1	duration_d30_t1	payTimes_d30_t1	payAmount_d30_t1	lastLoginTime_d30_t1	lastPayTime_d30_t1	//应用类型1
 * appType_d30_t2	validDays_d30_t2	loginTimes_d30_t2	duration_d30_t2	payTimes_d30_t2	payAmount_d30_t2	lastLoginTime_d30_t2	lastPayTime_d30_t2	//应用类型2
 * appType_d30_t3	validDays_d30_t3	loginTimes_d30_t3	duration_d30_t3	payTimes_d30_t3	payAmount_d30_t3	lastLoginTime_d30_t3	lastPayTime_d30_t3	//应用类型3
 * appType_d30_t4	validDays_d30_t4	loginTimes_d30_t4	duration_d30_t4	payTimes_d30_t4	payAmount_d30_t4	lastLoginTime_d30_t4	lastPayTime_d30_t4	//应用类型4
 * appType_d30_t5	validDays_d30_t5	loginTimes_d30_t5	duration_d30_t5	payTimes_d30_t5	payAmount_d30_t5	lastLoginTime_d30_t5	lastPayTime_d30_t5	//应用类型5
 * 
 * 
 * 2.UID+APPID 统计
 * uid	appId
 * validDays	loginTimes	duration	payTimes	payAmount	// 历史统计
 * validDays_d30	loginTimes_d30	duration_d30	payTimes_d30	payAmount_d30	// 最近30天统计
 * validDays_d07	loginTimes_d07	duration_d07	payTimes_d07	payAmount_d07	// 最近7天统计
 * duration_d30_h00	duration_d30_h01	...	duration_d30_h22	duration_d30_h23	// 24小时在线时长（最近30天）
 * payTimes_d30_h00	payTimes_d30_h01	...	payTimes_d30_h22	payTimes_d30_h23	// 24小时付费次数（最近30天）
 * duration_d30_weekday_h00	duration_d30_weekday_h01	...	duration_d30_weekday_h22	duration_d30_weekday_h23	// 24小时在线时长（最近30天工作日）
 * payTimes_d30_weekday_h00	payTimes_d30_weekday_h01	...	payTimes_d30_weekday_h22	payTimes_d30_weekday_h23	// 24小时付费次数（最近30天工作日）
 * duration_d30_weekend_h00	duration_d30_weekend_h01	...	duration_d30_weekend_h22	duration_d30_weekend_h23	// 24小时在线时长（最近30天周末）
 * payTimes_d30_weekend_h00	payTimes_d30_weekend_h01	...	payTimes_d30_weekend_h22	payTimes_d30_weekend_h23	// 24小时付费次数（最近30天周末）
 * 
 * @author sam.xie
 * @date 2015年7月30日 下午2:39:22
 * @version 1.0
 */
public class WHUidStatDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private String fileSuffix = "";
	private Date scheduleTime = null;
	private int statTime = 0;

	@Override
	protected void setup(Context context) {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_ROLLING)) {
			// UID 汇总统计
			OnlineAndPayRolling onlineAndPay = new OnlineAndPayRolling(1, paramArr);
			String uid = onlineAndPay.getKeys()[0];
			int lastUpdateTime = onlineAndPay.getLastUpdateTime();
			// 只统计更新时间在当天记录
			if (lastUpdateTime != statTime) {
				return;
			}

			int firstLoginTime = onlineAndPay.getFirstLoginTime();
			int intervalDays = (statTime - firstLoginTime) / WHConstants.SECONDS_IN_ONE_DAY + 1;
			int validDays_total = intervalDays;
			int validDays_30 = Math.min(30, intervalDays);
			int validDays_07 = Math.min(7, intervalDays);
			long loginTimes = onlineAndPay.getLoginTimes();
			long duration = onlineAndPay.getDuration();
			long payTimes = onlineAndPay.getPayTimes();
			float payAmount = onlineAndPay.getPayAmount();

			String[] loginDetail = resolveList(onlineAndPay.getLoginRecord(), false);
			String[] payDetail = resolveList(onlineAndPay.getPayRecord(), true);

			String[] keyArray = { uid };
			String[] valArray = { validDays_total + "", loginTimes + "", duration + "", payTimes + "", payAmount + "",
					validDays_30 + "", loginDetail[0], loginDetail[1], payDetail[0], payDetail[1], validDays_07 + "",
					loginDetail[2], loginDetail[3], payDetail[2], payDetail[3], };
			keyFields.setOutFields(keyArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_STAT_DAY);
			valFields.setOutFields(valArray);
			valFields.setSuffix("R");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING)) {
			// UID + APP 汇总统计
			OnlineAndPayRolling onlineAndPay = new OnlineAndPayRolling(4, paramArr);
			String uid = onlineAndPay.getKeys()[0];
			String appid = onlineAndPay.getKeys()[1];
			int lastUpdateTime = onlineAndPay.getLastUpdateTime();
			// 只统计更新时间在当天记录
			if (lastUpdateTime != statTime) {
				return;
			}

			int firstLoginTime = onlineAndPay.getFirstLoginTime();
			int intervalDays = (statTime - firstLoginTime) / WHConstants.SECONDS_IN_ONE_DAY + 1;
			int validDays_total = intervalDays;
			int validDays_30 = Math.min(30, intervalDays);
			int validDays_07 = Math.min(7, intervalDays);
			long loginTimes = onlineAndPay.getLoginTimes();
			long duration = onlineAndPay.getDuration();
			long payTimes = onlineAndPay.getPayTimes();
			float payAmount = onlineAndPay.getPayAmount();

			String[] loginDetail = resolveList(onlineAndPay.getLoginRecord(), false);
			String[] payDetail = resolveList(onlineAndPay.getPayRecord(), true);

			String[] keyArray = { uid, appid };
			String[] valArray = { validDays_total + "", loginTimes + "", duration + "", payTimes + "", payAmount + "",
					validDays_30 + "", loginDetail[0], loginDetail[1], payDetail[0], payDetail[1], validDays_07 + "",
					loginDetail[2], loginDetail[3], payDetail[2], payDetail[3], };
			keyFields.setOutFields(keyArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_STAT_DAY);
			valFields.setOutFields(valArray);
			valFields.setSuffix("R");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_HOUR)) {
			// UID 小时统计
			String uid = paramArr[0];
			keyFields.setOutFields(new String[] { uid });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_STAT_DAY);
			valFields.setOutFields(paramArr);
			valFields.setSuffix("H");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UIDAPP_HOUR)) {
			// UID + APP 小时统计
			String uid = paramArr[0];
			String appid = paramArr[1];
			keyFields.setOutFields(new String[] { uid, appid });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_STAT_DAY);
			valFields.setOutFields(paramArr);
			valFields.setSuffix("H");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_APPHABIT)) {
			// UID APP 使用偏好
			String uid = paramArr[0];
			keyFields.setOutFields(new String[] { uid });
			valFields.setOutFields(paramArr);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_STAT_DAY);
			valFields.setSuffix("A");
			context.write(keyFields, valFields);
		}
	}

	private String[] resolveList(String records, boolean isPay) {
		if (null == records || "-".equals(records)) {
			return new String[] { "0", "0", "0", "0" };
		}
		if (isPay) {
			int intValue1 = 0;
			int floatValue2 = 0;
			int intValue1_07 = 0;
			float floatValue2_07 = 0;
			for (String record : records.split(",")) {
				String[] items = record.split(":");
				int realDate = StringUtil.convertInt(items[0], 0);
				int payTimes = StringUtil.convertInt(items[1], 0);
				float payAmount = StringUtil.convertFloat(items[2], 0);
				intValue1 += payTimes;
				floatValue2 += payAmount;
				if (statTime - realDate <= WHConstants.SECONDS_IN_ONE_DAY * 6) { // 最近7天统计
					intValue1_07 += payTimes;
					floatValue2_07 += payAmount;
				}
			}
			return new String[] { intValue1 + "", floatValue2 + "", intValue1_07 + "", floatValue2_07 + "" };
		} else {
			int value1 = 0;
			int value2 = 0;
			int value1_07 = 0;
			int value2_07 = 0;
			for (String record : records.split(",")) {
				String[] items = record.split(":");
				int realDate = StringUtil.convertInt(items[0], 0);
				int loginTimes = StringUtil.convertInt(items[1], 0);
				int duration = StringUtil.convertInt(items[2], 0);
				value1 += loginTimes;
				value2 += duration;
				if (statTime - realDate <= WHConstants.SECONDS_IN_ONE_DAY * 6) { // 最近7天统计
					value1_07 += loginTimes;
					value2_07 += duration;
				}
			}
			return new String[] { value1 + "", value2 + "", value1_07 + "", value2_07 + "" };
		}
	}
}
