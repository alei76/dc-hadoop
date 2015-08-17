package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

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
 * 
 * 输入：
 * 1.在线按天统计
 * @see WHOnlineDayMapper
 * uid, accountId, appId, platform, channel, level, ts, ip, country, province, city, cleanFlag, 
 * totalLoginTimes, totalDuration, brand, resolution, os, mac, imei, imsi, idfa, [hour:times:duration...], dayOfWeek
 * 
 * 输出：
 * 1.帐号登陆表（天统计），取同一个角色的最大等级
 * accountId， uid, appId, role, level
 * 
 * @author sam.xie
 * @date 2015年7月13日 下午2:39:22
 * @version 1.0
 */
public class WHAccountDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
		// TODO 后面会加入role
		String role = "-";
		String level = log.getLevel() + "";

		// 账户登录表
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ACCOUNT_LOGIN);
		keyFields.setOutFields(new String[] { accountId, uid, appId, role });
		valFields.setOutFields(new String[] { level });
		context.write(keyFields, valFields);
	}

}
