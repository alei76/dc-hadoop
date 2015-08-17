package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：付费明细
 * 
 * 输入：
 * 1.用户付费日志payment.log
 * 
 * 输出：
 * 1.用户付费记录
 * uid	accountId	appId	platform	channel	level	ts	ip	country	province	city	cleanFlag
 * payTime	currencyAmount	currencyType	payType	virtualCurrencyAmount	orderId 
 * 
 * @author sam.xie
 * @date 2015年6月16日 下午4:39:22
 * @version 1.0
 */
public class WHPaymentDetailMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog log = null;
		try {
			log = new PaymentLog(paramsArr);
		} catch (Exception e) {
			return;
		}

		String ts = log.getTimestamp();// 这里可能同时存在Unix时间戳和毫秒时间戳
		ts = ts.length() > 10 ? ts.substring(0, 10) : ts;// // 毫秒时间戳转为秒时间戳，这里只保留高10位
		String uid = log.getUID(); // UID
		String accountId = log.getAccountID(); // AccountId
		String appId = StringUtil.getRealAppId(log.getAppID()); // appID
		String platform = log.getPlatform(); // 平台
		String channel = log.getChannel(); // 渠道
		// 渠道修正
		String reviseChannel = IOSChannelUtil.checkForiOSChannel(appId, uid, platform, channel);
		int level = log.getLevel();
		String ip = log.getExtend_4(); // ip
		String country = log.getCountry(); // 国家
		String province = log.getProvince(); // 省份
		String city = "-"; // 城市（占位）
		String cleanFlag = "-"; //清理标识（占位）
		
		int payTime = log.getPayTime(); // 付费时间
		float currencyAmount = log.getCurrencyAmount(); // 付费金额
		String currencyType = log.getCurrencyType(); // 货币类型
		String payType = log.getPayType(); // 付费类型
		float virtualCurrencyAmount = log.getVirtualCurrencyAmount(); // 虚拟金额
		String orderId = StringUtil.convertEmptyStr(log.getOrderId(), "-");// 订单号
//		String iapid = StringUtil.convertEmptyStr(log.getIapid(),"-");
		
		// 数据过滤
		if (appId.length() < 32 || log.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)) {
			return;
		}
		
		orderId = StringUtil.isEmpty(orderId) ? "-" : orderId;
		// 有一部份orderId包含转义\t,\n等数据的结果需要转义
		orderId = orderId.replace("\t", "-").replace("\n", "-");
		
		// 这里需要已uid和appid对数据进行清理
		String[] keyArray = { uid, appId};
		String[] valArray = { uid, accountId, appId, platform, reviseChannel, level + "", ts, ip, country, province, city, cleanFlag,
				payTime + "", currencyAmount + "", currencyType, payType, virtualCurrencyAmount + "", orderId + ""}; // 最后的“－”标识清洗结果，这里先占位,reduce阶段会处理 
		
		// 这里使用uid,appId作为key，只是方便过滤付费数据，但是最终还是需要输出明细（也就是val）
		keyFields.setOutFields(keyArray);
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_PAYMENT_DETAIL); 
		valFields.setOutFields(valArray);
		context.write(keyFields, valFields);
	}
}
