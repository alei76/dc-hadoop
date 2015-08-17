package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.warehouse.WHPaymentLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：付费按天汇总
 * 
 * 输入：@see {@link WHPaymentDetailMapper}
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag
 * payTime	currencyAmount	currencyType	payType	virtualCurrencyAmount	orderId
 * 
 * 输出：总付费次数，总付费金额，最大级别，日志类型（周末/工作日），小时付费次数:付费金额
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag
 * totalPayTimes	totalCurrencyAmount	-	-	-	[hour:payTimes:payAmount,...,hour:payTimes:payAmount]	dayOfWeek
 * 
 * @author sam.xie
 * @date 2015年6月26日 下午4:39:22
 * @version 1.0
 */
public class WHPaymentDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

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
		WHPaymentLog log = null;
		try {
			log = new WHPaymentLog(paramsArr);
		} catch (Exception e) {
			return;
		}
		// update by sam at 2015.07.14
		// 2.1之前作为DEUID_过滤，现在可用，无需过滤
		if ("-".equals(log.getCleanFlag()) || "2.1".equals(log.getCleanFlag())) {
			String uid = log.getUid();
			String accountId = log.getAccountId();
			String appId = log.getAppId();
			String platform = log.getPlatform() + "";
			String channel = log.getChannel();
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_PAYMENT_DAY);
			keyFields.setOutFields(new String[] { uid, accountId, appId, platform, channel, dayOfWeek + "" });
			valFields.setOutFields(paramsArr);
			context.write(keyFields, valFields);
		}
	}
}
