package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.model.warehouse.WHOnlineLog;
import net.digitcube.hadoop.model.warehouse.WHPaymentLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：【当天有活跃的用户】在最近30天的小时统计
 * 
 * 输入：
 * 1.最近30天在线统计	@see WHOnlineDayMapper
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * totalLoginTimes	totalDuration	brand	resolution	os	mac	imei	imsi	idfa	[hour:times:duration...], dayOfWeek
 * 
 * 2.最近30天付费统计 	@see WHPaymentDayMapper
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag
 * totalPayTimes	totalCurrencyAmount	-	-	-	[hour:payTimes,payAmount]	dayOfWeek
 * 
 * 
 * 输出：
 * 说明： 
 * weekType	1:所有日，2:工作日，3:周末
 * dim		1:登录次数，2:在线时长，3:付费次数，4:付费金额
 * 
 * 1.UID 小时分布		{@link Constants#SUFFIX_WAREHOUSE_UID_HOUR}
 * uid
 * weekType dim	value value_h00 value_h01 ... value_h22	value_h23	// 24小时指标统计
 * 
 * 2.UID+APP 小时分布	{@link Constants#SUFFIX_WAREHOUSE_UIDAPP_HOUR}
 * uid	appId
 * weekType dim	value value_h00 value_h01 ... value_h22	value_h23	// 24小时指标统计
 * 
 * 3.APP 小时分布		{@link Constants#SUFFIX_WAREHOUSE_APP_HOUR}
 * appId	platform	channel
 * weekType dim	value value_h00 value_h01 ... value_h22	value_h23	// 24小时指标统计
 * 
 * @author sam.xie
 * @date 2015年7月25日 下午2:23:33
 * @version 1.0
 */
public class WHUidHourMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Calendar calendar = Calendar.getInstance();
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
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_ONLINE_DAY)) {
			WHOnlineLog log = null;
			try {
				log = new WHOnlineLog(paramsArr);
			} catch (Exception e) {
				return;
			}
			String uid = log.getUid();
			String appId = log.getAppId();
			String platform = log.getPlatform() + "";
			String channel = log.getChannel();
			int realDate = truncate(log.getTs()); // 截取整天
			String hourRecord = log.getOperator(); // 这里表示小时记录,hour:
			String dayOfWeek = log.getNetType();

			// 日期间隔
			int intervalDays = (statTime - realDate) / WHConstants.SECONDS_IN_ONE_DAY;

			// 判断输入是否为最近的30天
			if (intervalDays < 0 || intervalDays > 29) {
				return;
			}

			// 设置value
			valFields.setSuffix("O");
			valFields.setOutFields(new String[] { intervalDays + "", dayOfWeek, hourRecord });

			// uid 分布统计
			keyFields.setOutFields(new String[] { uid });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_HOUR);
			context.write(keyFields, valFields);

			// uid + appid 分布统计
			keyFields.setOutFields(new String[] { uid, appId });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_HOUR);
			context.write(keyFields, valFields);

			// appid 分布统计
			keyFields.setOutFields(new String[] { appId, platform, channel });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APP_HOUR);
			context.write(keyFields, valFields);

		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_PAYMENT_DAY)) {
			WHPaymentLog log = null;
			try {
				log = new WHPaymentLog(paramsArr);
			} catch (Exception e) {
				return;
			}
			String uid = log.getUid();
			String appId = log.getAppId();
			String platform = log.getPlatform() + "";
			String channel = log.getChannel();
			int realDate = truncate(log.getTs()); // 截取整天
			String hourRecord = log.getVirtualCurrencyAmount(); // 这里表示小时记录,hour:
			String dayOfWeek = log.getOrderId();//

			// 日期间隔
			int intervalDays = (statTime - realDate) / WHConstants.SECONDS_IN_ONE_DAY;

			// 判断输入是否为最近的30天
			if (intervalDays < 0 || intervalDays > 29) {
				return;
			}

			// 设置value
			valFields.setSuffix("P");
			valFields.setOutFields(new String[] { intervalDays + "", dayOfWeek, hourRecord });

			// uid 分布统计
			keyFields.setOutFields(new String[] { uid });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_HOUR);
			context.write(keyFields, valFields);

			// uid + appid 分布统计
			keyFields.setOutFields(new String[] { uid, appId });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_HOUR);
			context.write(keyFields, valFields);

			// appid 分布统计
			keyFields.setOutFields(new String[] { appId, platform, channel });
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APP_HOUR);
			context.write(keyFields, valFields);
		}
	}

	/**
	 * <pre>
	 * 截取整天
	 * @author sam.xie
	 * @date 2015年7月21日 上午10:09:27
	 * @param time
	 * @return
	 */
	private int truncate(int unixTime) {
		calendar.setTimeInMillis(unixTime * 1000L);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
}
