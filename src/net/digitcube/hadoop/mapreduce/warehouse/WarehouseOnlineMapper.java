package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：在线表统计
 * 输入：
 * 1.用户每小时在线日志online.log
 * 
 * 
 * 输出1：用户完整的在线行为
 * Key：			UID,AppID,Platform,LoginTime
 * KeySuffix:	WAREHOUSE_ONLINE
 * Value：		Duration,ReceiveTime,IP,Country,Province,Brand,Resolution,OS,IMEI
 * 
 * 输出2：标识码
 * Key：			UID,MAC,IMEI
 * Key.Suffix:	WAREHOUSE_UID
 * Value:		
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午2:39:22
 * @version 1.0
 */
@Deprecated
public class WarehouseOnlineMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

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
		String receiveTime = log.getTimestamp();// 这里可能同时存在Unix时间戳和毫秒时间戳
		String appId = StringUtil.getRealAppId(log.getAppID()); // appID
		String uid = log.getUID(); // UID
		String platform = log.getPlatform(); // 平台
		String ip = log.getExtend_4(); // ip
		String country = log.getCountry(); // 国家
		String province = log.getProvince(); // 省份
		String brand = log.getBrand(); // 机型
		String resolution = log.getResolution(); // 分辨率
		String os = log.getOperSystem();// 操作系统
		int loginTime = log.getLoginTime(); // 登陆时间（Unix时间戳）
		int duration = log.getOnlineTime(); // 在线时长
		String mac = StringUtil.convertEmptyStr(log.getMac(), "-"); // MAC地址
		String imei = StringUtil.convertEmptyStr(log.getImei(), "-"); // IMEI

		// 数据过滤
		if (appId.length() < 32 || log.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)) {
			return;
		}
		receiveTime = receiveTime.length() > 10 ? receiveTime.substring(0, 10) : receiveTime;// // 毫秒时间戳转为秒时间戳，这里只保留高10位
		mac = StringUtil.isEmpty(mac) ? "-" : mac;
		imei = StringUtil.isEmpty(imei) ? "-" : imei;
		duration = (duration >= 24 * 3600 || duration <= 0) ? 1 : duration;
		String[] keyArray = { uid, appId, platform, loginTime + "" };
		String[] valArray = { duration + "", receiveTime, ip, country, province, brand, resolution, os, imei };

		// 在线记录
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ONLINE);
		keyFields.setOutFields(keyArray);
		valFields.setOutFields(valArray);
		context.write(keyFields, valFields);

		// 识别码记录
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID);
		keyFields.setOutFields(new String[] { uid, mac, imei });
		valFields.setOutFields(new String[] { "U" });
		context.write(keyFields, valFields);

	}
}
