package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：付费表
 * 
 * 输入：用户付费日志payment.log
 * 
 * 输出：用户付费记录
 * Key：		UID,AppID,Platform,PayTime,PayAmount,Currency,OrderId,ReceiveTime,IP,Country,Province,Brand,Resolution,OS,IMEI
 * Value：	NullWritable
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午4:39:22
 * @version 1.0
 */
@Deprecated
public class WarehousePaymentMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog log = null;
		try {
			log = new PaymentLog(paramsArr);
		} catch (Exception e) {
			return;
		}

		String ts = log.getTimestamp();// 这里可能同时存在Unix时间戳和毫秒时间戳
		String appId = StringUtil.getRealAppId(log.getAppID()); // appID
		String uid = log.getUID(); // UID
		String platform = log.getPlatform(); // 平台
		String ip = log.getExtend_4(); // ip
		String country = log.getCountry(); // 国家
		String province = log.getProvince(); // 省份
		String brand = log.getBrand(); // 机型
		String resolution = log.getResolution(); // 分辨率
		String os = log.getOperSystem();// 操作系统
		int payTime = log.getPayTime(); // 付费时间
		float payAmount = log.getCurrencyAmount(); // 付费金额
		String currency = log.getCurrencyType(); // 货币
		String orderId = StringUtil.convertEmptyStr(log.getOrderId(), "-");// 订单号
		String imei = StringUtil.convertEmptyStr(log.getImei(), "-");// IMEI

		// 数据过滤
		if (appId.length() < 32 || log.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)) {
			return;
		}

		ts = ts.length() > 10 ? ts.substring(0, 10) : ts;// // 毫秒时间戳转为秒时间戳，这里只保留高10位
		orderId = StringUtil.isEmpty(orderId) ? "-" : orderId;
		String[] keyArray = { uid, appId, platform, payTime + "", payAmount + "", currency, orderId + "", ts, ip,
				country, province, brand, resolution, os, imei };
		keyFields.setOutFields(keyArray);
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_PAYMENT);
		context.write(keyFields, NullWritable.get());
	}
}
