package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.LRULinkedHashMap;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午3:43:17 @copyrigt www.digitcube.net
 * 
 *          输入：用户24小时付费日志<br/>
 *          Timestamp APPID UID AccountID Platform Channel AccountType Gender
 *          Age GameServer Resolution OperSystem Brand NetType Country Province
 *          Operators CurrencyAmount VirtualCurrencyAmount Iapid CurrencyType
 *          PayType Level PayTime <br/>
 *          输出：<br/>
 *          key : appid,platform,accountid<br/>
 *          value:channel,accounttype,gender,age,gameserver,resolution,
 *          opersystem ,brand,nettype,country,province,operators,CurrencyAmount
 * 
 * Added by rickpan
 * 在对当天付费玩家去重的同时，统计当天付费玩家的在等级上的付费分布
 * 包括付费总金额和付费人次
 * 如，15 级的玩家总付费是 3w 元，付费人次为 1000（即人均付费为 30 元）
 * 
 */

public class PaymentDayMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	public final static int Index_CurrencyAmount = 17;

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
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog paymentLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			paymentLog = new PaymentLog(paymentArr);
		}catch(Exception e){
			e.printStackTrace();
			mapKeyObj.setOutFields(paymentArr);
			mapValueObj.setOutFields(new String[]{});
			mapKeyObj.setSuffix("ERROR_DATA");
			context.write(mapKeyObj, mapValueObj);
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(paymentLog.getAppID())){
			return;
		}
		
		//20140723: appId 中版本号，reduce 端处理时去取大版本号，其它信息揉合
		String[] appInfo = paymentLog.getAppID().split("\\|");
		String appId = appInfo[0];
		String appVersion = appInfo[1];

		// 20141212 : 
		// 从 hbase 中查找当前货币到目标货币的汇率并转化, 如果没找到则返回汇率为 1
		String rowkey = appId + "|" + paymentLog.getCurrencyType();
		Float exchangeRate = lurMap.get(rowkey);
		if(null == exchangeRate){
			exchangeRate = getExchangeRateFromHBase(rowkey);
			lurMap.put(rowkey, exchangeRate);
		}
		float exchangeCurrency = exchangeRate * paymentLog.getCurrencyAmount();
		paymentLog.setCurrencyAmount(exchangeCurrency);
				
		//真实区服
		String[] keyFields = new String[] { 
				//paymentLog.getAppID(),
				appId,
				paymentLog.getPlatform(),
				paymentLog.getAccountID(),
				paymentLog.getGameServer()
		};
		//全服
		String[] keyFields_AllGS = new String[] { 
				//paymentLog.getAppID(),
				appId,
				paymentLog.getPlatform(),
				paymentLog.getAccountID(),
				MRConstants.ALL_GAMESERVER
		};
		
		String[] valueFields = new String[] {
				/** 公用头部 **/
				paymentLog.getChannel(),
				paymentLog.getAccountType(),
				paymentLog.getGender(),
				paymentLog.getAge(),
				paymentLog.getGameServer(),
				paymentLog.getResolution(),
				paymentLog.getOperSystem(),
				paymentLog.getBrand(),
				paymentLog.getNetType(),
				paymentLog.getCountry(),
				paymentLog.getProvince(),
				paymentLog.getOperators(),
				""+paymentLog.getCurrencyAmount(),
				""+paymentLog.getLevel(), // pay level 用于首付级别分布统计
				""+paymentLog.getPayTime(), // pay time 用于首付级别分布统计
				appVersion
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		mapKeyObj.setSuffix(Constants.SUFFIX_PAYMENT_DAY); // set data flag for payment sum
		
		//Added at 20140606 : 为了在 reduce 中能够用 appId + UID 进行渠道修正而添加
		/*if(MRConstants.PLATFORM_iOS_STR.equals(paymentLog.getPlatform())){
			String UID = paymentLog.getUID();
			mapValueObj.setSuffix(UID);
		}else{
			mapValueObj.setSuffix("");
		}*/
		//各个平台都需要渠道修正
		mapValueObj.setSuffix(paymentLog.getUID());
		context.write(mapKeyObj, mapValueObj);
		
		//全服
		valueFields[4] = MRConstants.ALL_GAMESERVER;
		mapValueObj.setOutFields(valueFields);
		mapKeyObj.setOutFields(keyFields_AllGS);
		context.write(mapKeyObj, mapValueObj);
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
