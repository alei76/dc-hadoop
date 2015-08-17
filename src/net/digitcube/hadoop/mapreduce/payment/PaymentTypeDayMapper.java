package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.LRULinkedHashMap;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 输入：payment每个小时时间片原始日志
 * 
 * 输出：
 * 1.付费类型
 * Key			AppID,Platform,Channel,GameServer,PayType
 * Key.Suffix	PAYMENT_PAYTYPE_DAY
 * Value		Amount,AppVersion
 * 
 * 2.付费点
 * Key			AppID,Platform,Channel,GameServer,IAPID
 * Key.Suffix	PAYMENT_POINT_DAY
 * Value		Amount,AppVersion
 * 
 * 3.付费货币（货币）
 * Key			AppID,Platform,Channel,GameServer,Currency
 * Key.Suffix	PAYMENT_CURRENCY_DAY
 * Value		OriginalAmount,FinalAmount
 * 
 */
public class PaymentTypeDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	// 用于缓存从 hbase 查询返回来的汇率(1w 条记录占不到 1m 内存)
	LRULinkedHashMap<String, Float> lurMap = new LRULinkedHashMap<String, Float>(10000);

	public static final String DC_EXCHANGE_RATE = "dc_exchange_rate";
	public static final byte[] info = Bytes.toBytes("info");
	public static final byte[] q = Bytes.toBytes("q");
	public static final byte[] TB_DC_EXCHANGE_RATE = Bytes.toBytes(DC_EXCHANGE_RATE);
	HConnection conn = null;
	HTableInterface htable = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conn = HbasePool.getConnection();
		htable = conn.getTable(TB_DC_EXCHANGE_RATE);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		HbasePool.close(conn);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog paymentLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			paymentLog = new PaymentLog(paymentArr);
		} catch (Exception e) {
			return;
		}

		// 验证appId长度
		if (!FieldValidationUtil.validateAppIdLength(paymentLog.getAppID())) {
			return;
		}

		// appId 中版本号，reduce 端处理时去取大版本号，其它信息揉合
		String[] appInfo = paymentLog.getAppID().split("\\|");
		String appId = appInfo[0];
		String appVersion = appInfo[1];

		// 20141212 :
		// 从 hbase 中查找当前货币到目标货币的汇率并转化, 如果没找到则返回汇率为 1
		String rowkey = appId + "|" + paymentLog.getCurrencyType();
		Float exchangeRate = lurMap.get(rowkey);
		if (null == exchangeRate) {
			exchangeRate = getExchangeRateFromHBase(rowkey);
			lurMap.put(rowkey, exchangeRate);
		}
		float originalCurrency = paymentLog.getCurrencyAmount();
		float exchangeCurrency = exchangeRate * paymentLog.getCurrencyAmount();
		paymentLog.setCurrencyAmount(exchangeCurrency);

		/** 1.付费方式 */
		// 真实区服
		String[] keyFields = new String[] { paymentLog.getAppID(), paymentLog.getPlatform(), paymentLog.getChannel(),
				paymentLog.getGameServer(), paymentLog.getPayType() };
		// 全服
		String[] keyFields_AllGS = new String[] { paymentLog.getAppID(), paymentLog.getPlatform(),
				paymentLog.getChannel(), MRConstants.ALL_GAMESERVER, paymentLog.getPayType() };
		String[] valueFields = new String[] { paymentLog.getCurrencyAmount() + "" };
		writeFields(context, keyFields, valueFields, Constants.SUFFIX_PAYMENT_PAYTYPE_DAY, paymentLog);
		writeFields(context, keyFields_AllGS, valueFields, Constants.SUFFIX_PAYMENT_PAYTYPE_DAY, paymentLog);

		/** 2.付费点 */
		// 真实区服
		keyFields = new String[] { appId, paymentLog.getPlatform(), paymentLog.getChannel(),
				paymentLog.getGameServer(), paymentLog.getIapid() };
		// 全服
		keyFields_AllGS = new String[] { appId, paymentLog.getPlatform(), paymentLog.getChannel(),
				MRConstants.ALL_GAMESERVER, paymentLog.getIapid() };
		valueFields = new String[] { paymentLog.getCurrencyAmount() + "", appVersion, paymentLog.getAccountID() };
		writeFields(context, keyFields, valueFields, Constants.SUFFIX_PAYMENT_POINT_DAY, paymentLog);
		writeFields(context, keyFields_AllGS, valueFields, Constants.SUFFIX_PAYMENT_POINT_DAY, paymentLog);

		/** 3.付费金额-货币分布 */
		// 真实区服
		keyFields = new String[] { appId, paymentLog.getPlatform(), paymentLog.getChannel(),
				paymentLog.getGameServer(), paymentLog.getCurrencyType() };
		// 全服
		keyFields_AllGS = new String[] { appId, paymentLog.getPlatform(), paymentLog.getChannel(),
				MRConstants.ALL_GAMESERVER, paymentLog.getCurrencyType() };
		valueFields = new String[] { originalCurrency + "", +exchangeCurrency + "", appVersion }; // 原始金额，汇率转换后金额，版本
		writeFields(context, keyFields, valueFields, Constants.SUFFIX_PAYMENT_CURRENCY_DAY, paymentLog);
		writeFields(context, keyFields_AllGS, valueFields, Constants.SUFFIX_PAYMENT_CURRENCY_DAY, paymentLog);

		/** 4.加入移动运营商付费金额 */
		String simCode = paymentLog.getSimOpCode();
		simCode = simCode == null ? "-" : simCode;
		// 真实区服
		keyFields = new String[] { paymentLog.getAppID(), paymentLog.getPlatform(), paymentLog.getChannel(),
				paymentLog.getGameServer(), simCode };
		// 全服
		keyFields_AllGS = new String[] { paymentLog.getAppID(), paymentLog.getPlatform(), paymentLog.getChannel(),
				MRConstants.ALL_GAMESERVER, simCode };

		valueFields = new String[] { paymentLog.getCurrencyAmount() + "" };
		writeFields(context, keyFields, valueFields, Constants.SUFFIX_PAYMENT_SIMCODE_DAY, paymentLog);
		writeFields(context, keyFields_AllGS, valueFields, Constants.SUFFIX_PAYMENT_SIMCODE_DAY, paymentLog);

	}

	public void writeFields(Context context, String[] keys, String[] values, String suffix, PaymentLog paymentLog)
			throws IOException, InterruptedException {
		// 为了在 reduce 中能够用 appId + UID 进行渠道修正而添加
		if (MRConstants.PLATFORM_iOS_STR.equals(paymentLog.getPlatform())) {
			String UID = paymentLog.getUID();
			mapValueObj.setSuffix(UID);
		} else {
			mapValueObj.setSuffix("");
		}
		mapKeyObj.setSuffix(suffix);
		mapKeyObj.setOutFields(keys);
		mapValueObj.setOutFields(values);
		context.write(mapKeyObj, mapValueObj);
	}

	private float getExchangeRateFromHBase(String rowkey) {
		float exchangeRate = 1.0f;

		try {
			Get get = new Get(Bytes.toBytes(rowkey));
			Result result = htable.get(get);

			// 符合下列条件之一时视为新增玩家
			// a,表中不存在(当天小时任务计算时表中还不存在，次日凌晨 MR 任务才会把玩家加入到表中)
			// b,表中存在,并且新增日期和统计日期相同
			if (null != result) {
				byte[] rate = result.getValue(info, q);
				if (null != rate) {
					exchangeRate = Bytes.toFloat(rate);
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return exchangeRate;
	}
}
