package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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
 * 付费明细
 */
public class PaymentDetailMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private String appIdFilter = "DFFCC14D767F710B79006CC5AD2908CF"; // 我是死神
	private Calendar cal = Calendar.getInstance();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	// private String pathSuffix = "";

	protected void setup(Context context) {
		appIdFilter = context.getConfiguration().get("appIdFilter", appIdFilter);
		// String path = ((FileSplit) context.getInputSplit()).getPath().toString();
		// System.out.println("path->" + path);
		// System.out.println("file->" + ((FileSplit) context.getInputSplit()).getPath().getName());
		// hdfs://dcnamenode1:9000/data/logserver-ext/payment/2015/07/16/*/input/
		// pathSuffix = path.split("/")[6] + path.split("/")[7] + path.split("/")[8];
	}

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

		if (!appIdFilter.contains(appId)) {
			return;
		}

		String platform = log.getPlatform(); // 平台
		String channel = log.getChannel(); // 渠道
		// 渠道修正
		String reviseChannel = IOSChannelUtil.checkForiOSChannel(appId, uid, platform, channel);
		int level = log.getLevel();
		String ip = log.getExtend_4(); // ip
		String country = log.getCountry(); // 国家
		String province = log.getProvince(); // 省份
		String city = "-"; // 城市（占位）
		String cleanFlag = "-"; // 清理标识（占位）

		int payTime = log.getPayTime(); // 付费时间
		float currencyAmount = log.getCurrencyAmount(); // 付费金额
		String currencyType = log.getCurrencyType(); // 货币类型
		String payType = log.getPayType(); // 付费类型
		float virtualCurrencyAmount = log.getVirtualCurrencyAmount(); // 虚拟金额
		String orderId = StringUtil.convertEmptyStr(log.getOrderId(), "-");// 订单号
		String iapid = StringUtil.convertEmptyStr(log.getIapid(), "-");

		// 数据过滤
		if (appId.length() < 32 || log.getAccountID().equals(Constants.NO_LOGIN_ACCOUNTID)) {
			return;
		}

		orderId = StringUtil.isEmpty(orderId) ? "-" : orderId;
		// 有一部份orderId包含转义\t,\n等数据的结果需要转义
		orderId = orderId.replace("\t", "-").replace("\n", "-");
		
		cal.setTimeInMillis(Long.parseLong(ts) * 1000);
		String dateSuffix = sdf.format(cal.getTime());

		// 这里需要已uid和appid对数据进行清理
		String[] keyArray = { uid, appId };
		String[] valArray = { uid, accountId, appId, platform, reviseChannel, level + "", dateSuffix, ip, country, province,
				city, cleanFlag, payTime + "", currencyAmount + "", currencyType, payType, virtualCurrencyAmount + "",
				orderId, iapid };
		// 这里使用uid,appId作为key，只是方便过滤付费数据，但是最终还是需要输出明细（也就是val）
		keyFields.setOutFields(keyArray);
		keyFields.setSuffix("PAYMENT_DETAIL_" + dateSuffix);
		valFields.setOutFields(valArray);

		context.write(keyFields, valFields);

	}
}
