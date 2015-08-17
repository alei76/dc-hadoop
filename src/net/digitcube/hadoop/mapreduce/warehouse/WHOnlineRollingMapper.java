package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.warehouse.WHOnlineLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：在线表统计
 * 
 * 输入：
 * 1.在线按天统计
 * @see WHOnlineDayMapper
 * uid, accountId, appId, platform, channel, level, ts, ip, country, province, city, cleanFlag, 
 * totalLoginTimes, totalDuration, brand, resolution, os, mac, imei, imsi, idfa, [hour:times:duration...], dayOfWeek
 * 
 * 2.前一天的历史滚存
 * @see WHOnlineRollingMapper
 * 
 * 输出：（创建时间使用首次在线时间）
 * 1.账户表（滚存）
 * accountId	uid	appId	createTime
 * 
 * 2.设备表（滚存）
 * uid	mac	idfa	imei	platform	brand	resolution	createTime
 * 
 * 3.移动用户识别码表（滚存）
 * uid	imsi	createTime
 * 
 * @author sam.xie
 * @date 2015年6月30日 下午2:39:22
 * @version 1.0
 */
public class WHOnlineRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	private String fileSuffix = "";
	private int statDate = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		statDate = getStatDate(context);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_ONLINE_DAY)) {
			String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
			WHOnlineLog log = null;
			try {
				log = new WHOnlineLog(paramsArr);
			} catch (Exception e) {
				return;
			}
			String uid = log.getUid();
			String accountId = log.getAccountId();
			String appId = log.getAppId();
			String mac = log.getMac();
			String imei = log.getImei();
			String idfa = log.getIdfa();
			String imsi = log.getImsi();
			byte platform = log.getPlatform();
			String brand = log.getBrand();
			String resolution = log.getResolution();
			int loginDate = StringUtil.truncateDate(log.getTs(), statDate);

			// 账户
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ACCOUNT_ROLLING);
			keyFields.setOutFields(new String[] { accountId, uid, appId });
			valFields.setOutFields(new String[] { loginDate + "" });
			context.write(keyFields, valFields);

			// UID
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_DEVICE_ROLLING);
			keyFields.setOutFields(new String[] { uid, mac, idfa, imei, platform + "", brand, resolution });
			valFields.setOutFields(new String[] { loginDate + "" });
			context.write(keyFields, valFields);

			// UID + IMSI
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_IMSI_ROLLING);
			keyFields.setOutFields(new String[] { uid, imsi });
			valFields.setOutFields(new String[] { loginDate + "" });
			context.write(keyFields, valFields);
		} else if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_ACCOUNT_ROLLING)) {
			String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
			String accountId = paramsArr[0];
			String uid = paramsArr[1];
			String appId = paramsArr[2];
			String firstLoginDate = paramsArr[3];
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ACCOUNT_ROLLING);
			keyFields.setOutFields(new String[] { accountId, uid, appId });
			valFields.setOutFields(new String[] { firstLoginDate });
			context.write(keyFields, valFields);
		} else if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_DEVICE_ROLLING)) {
			String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
			String uid = paramsArr[0];
			String mac = paramsArr[1];
			String idfa = paramsArr[2];
			String imei = paramsArr[3];
			String platform = paramsArr[4];
			String brand = paramsArr[5];
			String resolution = paramsArr[6];
			String firstLoginDate = paramsArr[7];
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_DEVICE_ROLLING);
			keyFields.setOutFields(new String[] { uid, mac, idfa, imei, platform, brand, resolution });
			valFields.setOutFields(new String[] { firstLoginDate });
			context.write(keyFields, valFields);
		} else if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_IMSI_ROLLING)) {
			String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
			String uid = paramsArr[0];
			String imsi = paramsArr[1];
			String firstLoginDate = paramsArr[2];
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_IMSI_ROLLING);
			keyFields.setOutFields(new String[] { uid, imsi });
			valFields.setOutFields(new String[] { firstLoginDate });
			context.write(keyFields, valFields);
		}

	}

	private int getStatDate(Context cxt) {
		Date date = ConfigManager.getInitialDate(cxt.getConfiguration());
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}
}
