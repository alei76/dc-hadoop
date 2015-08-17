package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.warehouse.WHOnlineLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：在线表统计
 * 输入：
 * 1.@see {@link WHOnlineDetailMapper}
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * loginTime	duration	brand	resolution	os	mac	imei	imsi	idfa	operator	netType
 * 
 * 输出：总登录次数，总在线时长，最大级别，日期类型（周末/工作日），小时登录次数+在线时长
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * totalLoginTimes	totalDuration	brand	resolution	os	mac	imei	imsi	idfa	[hour:times:duration,...,hour:times:duration]	dayOfWeek
 * 
 * @author sam.xie
 * @date 2015年6月26日 下午2:39:22
 * @version 1.0
 */
public class WHOnlineDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Date scheduleTime = null;
	private int dayOfWeek = 0;

	@Override
	protected void setup(Context context) {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		// 1,2,3,4,5,6,7分别对应周日，周一，周二，周三，周四，周五，周六
		dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		WHOnlineLog log = null;
		try {
			log = new WHOnlineLog(paramsArr);
		} catch (Exception e) {
			return;
		}
		// update by sam at 2015.07.14
		// 2.1之前作为DEUID_过滤，现在可用，无需过滤
		if ("-".equals(log.getCleanFlag()) || "2.1".equals(log.getCleanFlag())) { // 过滤清理数据
			String uid = log.getUid();
			String accountId = log.getAccountId();
			String appId = log.getAppId();
			String platform = log.getPlatform() + "";
			String channel = log.getChannel();
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ONLINE_DAY);
			keyFields.setOutFields(new String[] { uid, accountId, appId, platform, channel, dayOfWeek + "" });
			valFields.setOutFields(paramsArr);
			context.write(keyFields, valFields);
		}
	}
}
