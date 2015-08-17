package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：付费表
 * 
 * 输入：
 * 1.@see WHUidRollingMapper 
 * uid	appId
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * 2.@see WHOnlineDayMapper
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * totalLoginTimes	totalDuration	brand	resolution	os	mac	imei	imsi	idfa	[hour:times:duration,...,hour:times:duration]	dayOfWeek
 * 
 * 3.@see WHPaymentDayMapper
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag
 * totalPayTimes	totalCurrencyAmount	-	-	-	[hour:payTimes:payAmount,...,hour:payTimes:payAmount]	dayOfWeek
 * 
 * 输出：将UserInfoRollingDayMapper中的channel 和 platform取出来 存到WHUidRollingMapper中
 * uid	appId	platform	channel // 这里保存首次登录或付费的平台，渠道，后面不再变更
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:loginTimes:duration,...,loginTime:loginTimes:duration]
 * 30PayMark	30PayList[payTime:payTimes:payAmount,...,payTime:payTimes:payAmount]
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午4:39:22
 * @version 1.0
 */
public class TmpWHRollingMergeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	public Map<String, String> macMap = new HashMap<String, String>();
	public String fileSuffix = "";
	private Date scheduleTime = null;

	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		// 重复的UID,appID清理，同时加上platform,channel
		
		String[] targetArr = new String[paramArr.length - 2];
		System.arraycopy(paramArr, 0, targetArr, 0, 4);
		System.arraycopy(paramArr, 6, targetArr, 4, targetArr.length - 4);
		keyFields.setOutFields(targetArr);
		context.write(keyFields, NullWritable.get());
//		if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_UIDAPP_ROLLING)) {
//			String uid = paramsArr[0];
//			String appId = paramsArr[1];
//			keyFields.setOutFields(new String[] { uid, appId });
//			valFields.setOutFields(paramsArr);
//			valFields.setSuffix("R");
//			context.write(keyFields, valFields);
//		} else if (fileSuffix.endsWith(Constants.SUFFIX_WAREHOUSE_ONLINE_DAY)) {
//			String uid = paramsArr[0];
//			String accountId = paramsArr[1];
//			String appId = paramsArr[2];
//			String platform = paramsArr[3];
//			String channel = paramsArr[4];
//			String ts = paramsArr[6];
//			keyFields.setOutFields(new String[] { uid, appId });
//			valFields.setOutFields(new String[] { ts, platform, channel });
//			valFields.setSuffix("D");
//			context.write(keyFields, valFields);
//		}
	}
}
