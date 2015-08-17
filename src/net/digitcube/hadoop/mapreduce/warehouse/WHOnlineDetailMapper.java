package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：在线明细
 * 
 * 输入：
 * 1.用户在线日志online.log
 * 
 * 输出:
 * 1.用户完整的在线行为（按登录时间去重）
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag 
 * loginTime	duration	brand	resolution	os	mac	imei	imsi	idfa	operator	netType
 * 
 * @author sam.xie
 * @date 2015年6月16日 下午2:39:22
 * @version 1.0
 */
public class WHOnlineDetailMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog log = null;
		try {
			log = new OnlineLog(paramsArr);

		} catch (Exception e) {
			return;
		}
		String ts = log.getTimestamp();// 这里可能同时存在Unix时间戳和毫秒时间戳
		ts = ts.length() > 10 ? ts.substring(0, 10) : ts;// // 毫秒时间戳转为秒时间戳，这里只保留高10位
		String uid = log.getUID(); // UID
		String accountId = log.getAccountID();
		String appId = StringUtil.getRealAppId(log.getAppID()); // appID
		String platform = log.getPlatform(); // 平台
		String channel = log.getChannel(); // 渠道
		// 渠道修正
		String reviseChannel = IOSChannelUtil.checkForiOSChannel(appId, uid, platform, channel);
		// 数据过滤
		if (appId.length() < 32 || log.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)) {
			return;
		}
		int level = log.getLevel();
		String ip = log.getExtend_4(); // ip
		String country = log.getCountry(); // 国家
		String province = log.getProvince(); // 省份
		String city = "-"; // 城市（占位）
		String cleanFlag = "-"; //清理标识（占位）
		int loginTime = log.getLoginTime(); // 登陆时间（Unix时间戳）
		int duration = log.getOnlineTime(); // 在线时长
		// 时长大于 1 天则取 1 秒,时长小于等于 0 则取 1秒
		duration = (duration >= 24 * 3600 || duration <= 0) ? 1 : duration;
		String brand = log.getBrand(); // 机型
		String resolution = log.getResolution(); // 分辨率
		String os = log.getOperSystem();// 操作系统
		String mac = StringUtil.convertEmptyStr(log.getMac(), "-"); // MAC
		String imei = StringUtil.convertEmptyStr(log.getImei(), "-"); // IMEI
		String imsi = StringUtil.convertEmptyStr(log.getImsi(), "-"); // IMSI
		String idfa = StringUtil.convertEmptyStr(log.getIdfa(), "-"); // IDFA
		String operator = StringUtil.convertEmptyStr(log.getOperators(), "-"); // Operator 
		String netType = StringUtil.convertEmptyStr(log.getNetType(), "-"); // netType
		String[] keyArray = { uid, accountId, appId, platform, loginTime + "" };
		String[] valArray = { uid, accountId, appId, platform, reviseChannel, level + "", ts, ip, country, province, city, cleanFlag, 
				loginTime + "", duration + "", brand, resolution, os, mac, imei, imsi, idfa, operator, netType};

		// 在线记录
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ONLINE_DETAIL);
		keyFields.setOutFields(keyArray);
		valFields.setOutFields(valArray);
		context.write(keyFields, valFields);

	}
}
