package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.LRULinkedHashMap;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.model.PaymentLog;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class H5PaymentDayReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, NullWritable> {
	
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
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int i = 0;
		String pureAppId = key.getOutFields()[i++];
		String platform = key.getOutFields()[i++];
		String accountId = key.getOutFields()[i++];
		
		PaymentLog defaultVal = null;
		String maxVersion = "";
		float totalCurrency = 0;
		int totalPaytimes = 0;
		for(Text pay : values){
			
			PaymentLog paymentLog = null;
			try{
				String[] paymentArr = pay.toString().split(MRConstants.SEPERATOR_IN);
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				paymentLog = new PaymentLog(paymentArr);
			}catch(Exception e){
				continue;
			}
			
			totalPaytimes++;
			
			// 20141212 : 
			// 从 hbase 中查找当前货币到目标货币的汇率并转化, 如果没找到则返回汇率为 1
			String rowkey = pureAppId + "|" + paymentLog.getCurrencyType();
			Float exchangeRate = lurMap.get(rowkey);
			if(null == exchangeRate){
				exchangeRate = getExchangeRateFromHBase(rowkey);
				lurMap.put(rowkey, exchangeRate);
			}
			float exchangeCurrency = exchangeRate * paymentLog.getCurrencyAmount();
			paymentLog.setCurrencyAmount(exchangeCurrency);
			
			totalCurrency += paymentLog.getCurrencyAmount();
			
			String curVersion = paymentLog.getAppID().split("\\|")[1];
			if(maxVersion.compareTo(curVersion) < 0){
				maxVersion = curVersion;
			}
			
			if(null == defaultVal || paymentLog.getPayTime() > defaultVal.getPayTime()){
				defaultVal = paymentLog;
			}
			
		}
		
		String H5_PromotionAPP = defaultVal.getH5PromotionApp();
		String H5_DOMAIN = defaultVal.getH5Domain();
		String H5_REF = defaultVal.getH5Refer();
		
		String[] valFields = new String[]{
				pureAppId + "|" + maxVersion,
				platform,
				(null == H5_PromotionAPP) ? "-" : H5_PromotionAPP,
				(null == H5_DOMAIN) ? "-" : H5_DOMAIN,
				(null == H5_REF) ? "-" : H5_REF,
				accountId,
				totalCurrency+"",
				totalPaytimes+""
		};	
		key.setSuffix(Constants.SUFFIX_PAYMENT_DAY);
		key.setOutFields(valFields);
		context.write(key, NullWritable.get());
	}
	
	private float getExchangeRateFromHBase(String rowkey){
		float exchangeRate = 1.0f;
		
		try {
			Get get = new Get(Bytes.toBytes(rowkey));
			Result result = htable.get(get);
			
			//符合下列条件之一时视为新增玩家
			//a,表中不存在(当天小时任务计算时表中还不存在，次日凌晨  MR 任务才会把玩家加入到表中)
			//b,表中存在,并且新增日期和统计日期相同
			if(null != result){
				byte[] rate = result.getValue(info, q);
				if(null != rate){
					exchangeRate = Bytes.toFloat(rate);
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return exchangeRate;
	}
}
