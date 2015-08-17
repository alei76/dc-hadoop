package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
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
 * changlog
 * update by sam 2015-07-27 10:24:28
 * 所有滚存加入最近30天付费统计
 * 
 * update by sam 2015-08-04 10:29:53
 * value输出中加入 platform, channel（取首次在线或付费时的platform, channel）
 * 
 * 输入：
 * 1.在线天统计	@see WHOnlineDayMapper
 * 2.付费天统计	@see WHPaymentDayMapper
 * 3.滚存		@see WHUidRollingMapper
 * 
 * 输出：
 * 1.UID Tracking（滚存）
 * uid
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * 2.UID+APP Tracking（滚存）
 * uid	appId	platform	channel // 这里保存首次登录或付费的平台，渠道
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * 3.APP Tracking（滚存）
 * appId	platform	channel
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * @author sam.xie
 * @date 2015年7月20日 下午2:39:22
 * @version 1.0
 */
public class WHUidRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

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
		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_ONLINE_DAY)) {
			String uid = paramArr[WHConstants.IDX_UID];
			String appId = paramArr[WHConstants.IDX_APPID];
			String platform = paramArr[WHConstants.IDX_PLATFORM];
			String channel = paramArr[WHConstants.IDX_CHANNEL];
			int realTime = StringUtil.truncateDate(paramArr[WHConstants.IDX_TS], statTime);
			String totalLoginTimes = paramArr[WHConstants.IDX_ONLINE_TOTALLOGINTIMES];
			String totalDuration = paramArr[WHConstants.IDX_ONLINE_TOTALDURATION];

			// UID 统计
			String[] keyArray = { uid };
			String[] valArray = { realTime + "", totalLoginTimes, totalDuration, platform, channel };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_ROLLING);
			valFields.setSuffix("O");
			context.write(keyFields, valFields);

			// UID + APP 统计（ 加入platform ,channel统计）
			keyArray = new String[] { uid, appId };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING);
			valFields.setSuffix("O");
			context.write(keyFields, valFields);

			// APP 统计
			keyArray = new String[] { appId, platform, channel };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APP_ROLLING);
			valFields.setSuffix("O");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_PAYMENT_DAY)) { // 付费天统计
			String uid = paramArr[WHConstants.IDX_UID];
			String appId = paramArr[WHConstants.IDX_APPID];
			String platform = paramArr[WHConstants.IDX_PLATFORM];
			String channel = paramArr[WHConstants.IDX_CHANNEL];
			int realTime = StringUtil.truncateDate(paramArr[WHConstants.IDX_TS], statTime);
			String totalPayTimes = paramArr[WHConstants.IDX_PAYMENT_TOTALPAYTIMES];
			String totalPayAmount = paramArr[WHConstants.IDX_PAYMENT_TOTALPAYAMOUNT];

			// UID 统计
			String[] keyArray = { uid };
			String[] valArray = { realTime + "", totalPayTimes, totalPayAmount, platform, channel };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_ROLLING);
			valFields.setSuffix("P");
			context.write(keyFields, valFields);

			// UID + APP 统计
			keyArray = new String[] { uid, appId };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING);
			valFields.setSuffix("P");
			context.write(keyFields, valFields);

			// APP 统计
			keyArray = new String[] { appId, platform, channel };
			keyFields.setOutFields(keyArray);
			valFields.setOutFields(valArray);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APP_ROLLING);
			valFields.setSuffix("P");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UID_ROLLING)) { // UID 滚存
			String uid = paramArr[0];
			keyFields.setOutFields(new String[] { uid });
			valFields.setOutFields(paramArr);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_ROLLING);
			valFields.setSuffix("R");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING)) { // UID + APP滚存
			String uid = paramArr[0];
			String appId = paramArr[1];
			keyFields.setOutFields(new String[] { uid, appId });
			valFields.setOutFields(paramArr);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING);
			valFields.setSuffix("R");
			context.write(keyFields, valFields);
		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_APP_ROLLING)) { // APP 滚存
			String appId = paramArr[0];
			String platform = paramArr[1];
			String channel = paramArr[2];
			keyFields.setOutFields(new String[] { appId, platform, channel });
			valFields.setOutFields(paramArr);
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APP_ROLLING);
			valFields.setSuffix("R");
			context.write(keyFields, valFields);
		}
	}
}
