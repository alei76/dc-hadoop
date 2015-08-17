package net.digitcube.hadoop.tmp;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 从滚存中过滤指定日期范围内的数据
 */
public class UserInfoRollingFilterMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = new Date();

	String appIdFilter = "F85D1E6AEDFDC3D32DD021A787257216";
	String method = "regTimeFilter";
	int date_20141201 = 1417363200;
	int date_20150101 = 1420041600;
	int date_20150201 = 1422720000;
	int date_20150301 = 1425139200;
	int date_20150401 = 1427817600;
	int date_20150501 = 1430409600;
	int date_20150601 = 1433088000;
	int date_20150701 = 1435680000;

	int startTime = date_20150401;
	int endTime = date_20150501;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		appIdFilter = context.getConfiguration().get("appIdFilter", appIdFilter);
		method = context.getConfiguration().get("method", method);
		startTime = context.getConfiguration().getInt("startTime", startTime);
		endTime = context.getConfiguration().getInt("endTime", endTime);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(scheduleTime, paraArr);
		if (method.equals("lastLoginTimeFilter")) {
			lastLoginTimeFilter(paraArr, userInfoRollingLog, context);
		} else if (method.equals("regTimeFilter")) {
			regTimeFilter(paraArr, userInfoRollingLog, context);
		}

	}

	protected void lastLoginTimeFilter(String[] paraArr, UserInfoRollingLog userInfoRollingLog, Context context)
			throws IOException, InterruptedException {
		int date = userInfoRollingLog.getPlayerDayInfo().getLastLoginDate();
		if (date < date_20141201) { // 只输出最后登录时间在指定时间之前的记录
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix("ROLLING_FILTER_LASTLOGINTIME_" + "0" + "-" + date_20141201);
			// mapKeyObj.setSuffix(appId);
			context.write(mapKeyObj, NullWritable.get());
		} else if (date >= date_20141201 && date < date_20150101) {
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix("ROLLING_FILTER_LASTLOGINTIME_" + date_20141201 + "-" + date_20150101);
			// mapKeyObj.setSuffix(appId);
			context.write(mapKeyObj, NullWritable.get());
		} else if (date >= date_20150101 && date < date_20150201) {
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix("ROLLING_FILTER_LASTLOGINTIME_" + date_20150101 + "-" + date_20150201);
			// mapKeyObj.setSuffix(appId);
			context.write(mapKeyObj, NullWritable.get());
		}
	}

	protected void regTimeFilter(String[] paraArr, UserInfoRollingLog userInfoRollingLog, Context context)
			throws IOException, InterruptedException {
		int date = userInfoRollingLog.getPlayerDayInfo().getRegTime();
		if (userInfoRollingLog.getAppID().contains("F85D1E6AEDFDC3D32DD021A787257216")) {
			if (date >= startTime && date < endTime) { // 获取每个月的新增玩家
				mapKeyObj.setOutFields(paraArr);
				mapKeyObj.setSuffix("ROLLING_FILTER_REGTIME_" + startTime + "-" + endTime);
				// mapKeyObj.setSuffix(appId);
				context.write(mapKeyObj, NullWritable.get());
			}
		}
	}
}
