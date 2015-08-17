package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OnlineAndPayInHbaseMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private Text valObj = new Text();
	
	private String tableName = null;
	private String fileName = "";
	private String yyyyMMdd = "";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
	private DecimalFormat onlineTimedf = new DecimalFormat();
	private DecimalFormat paymentdf = new DecimalFormat();
	
	private static final int dayMaxOnlineTime = 60*60*24;
	private static final int dayMaxPayment = 999999999;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		Calendar cal = Calendar.getInstance();
		if (date != null) {
			cal.setTime(date);
		}
		cal.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		yyyyMMdd = sdf.format(cal.getTime());
		
		// 一天的最大在线时间为：24*60*60 = 86400 秒
		// 所以 onlineTimedf 最多 5 位即可
		onlineTimedf.setMinimumIntegerDigits(5);
		onlineTimedf.setGroupingUsed(false);
		
		// 假设一天的最大付费金额为 9 亿
		// 所以 paymentdf 最多 9 位
		paymentdf.setMinimumIntegerDigits(9);
		paymentdf.setGroupingUsed(false);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		if(fileName.endsWith(Constants.SUFFIX_ONLINE_DAY)){
			OnlineDayLog onlineDayLog = new OnlineDayLog(array);
			
			//只统计全服的记录
			if(!MRConstants.ALL_GAMESERVER.equals(onlineDayLog.getExtend().getGameServer())){
				return;
			}
			
			String appId = onlineDayLog.getAppID().split("\\|")[0];
			String platform = onlineDayLog.getPlatform();
			String accountId = onlineDayLog.getAccountID();
			valObj.set(accountId);
			
			//当天在线时长
			int totalOnlineTime = onlineDayLog.getTotalOnlineTime();
			int timeRange = getOnlineTimeRange(totalOnlineTime);
			String onlineTimeRange = onlineTimedf.format(timeRange);
			keyObj.setOutFields(new String[]{
					appId,
					platform,
					yyyyMMdd,
					onlineTimeRange
			});
			tableName = context.getConfiguration().get("hbase.tablename.dayonlinetime");
			keyObj.setSuffix(tableName);
			context.write(keyObj, valObj);
			
			//地区
			String province = onlineDayLog.getExtend().getProvince();
			keyObj.setOutFields(new String[]{
					appId,
					platform,
					yyyyMMdd,
					province
			});
			tableName = context.getConfiguration().get("hbase.tablename.dayarea");
			keyObj.setSuffix(tableName);
			context.write(keyObj, valObj);
			
			//渠道
			String channel = onlineDayLog.getExtend().getChannel();
			keyObj.setOutFields(new String[]{
					appId,
					platform,
					yyyyMMdd,
					channel
			});
			tableName = context.getConfiguration().get("hbase.tablename.daychannel");
			keyObj.setSuffix(tableName);
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(Constants.SUFFIX_PAYMENT_DAY)){
			
			PaymentDayLog paymentDayLog = new PaymentDayLog(array);
			//只统计全服的记录
			if(!MRConstants.ALL_GAMESERVER.equals(paymentDayLog.getExtend().getGameServer())){
				return;
			}
			
			String appId = paymentDayLog.getAppID().split("\\|")[0];
			String platform = paymentDayLog.getPlatform();
			String accountId = paymentDayLog.getAccountID();
			valObj.set(accountId);
			
			//当天付费
			float payCurrency = paymentDayLog.getCurrencyAmount();
			int payRange = getPaymentRange(payCurrency);
			String paymentRange = paymentdf.format(payRange);
			keyObj.setOutFields(new String[]{
					appId,
					platform,
					yyyyMMdd,
					paymentRange
			});
			tableName = context.getConfiguration().get("hbase.tablename.daypayment");
			keyObj.setSuffix(tableName);
			context.write(keyObj, valObj);
		}
	}
	
	/**
	 * onlineTime <= 10m, 步长 = 10s
	 * 10m < onlineTime <= 60m, 步长 = 60s
	 * onlineTime > 60m, 步长 = 10m
	 * 
	 * 返回该步长区间大的值
	 * 
	 * @param onlineTime
	 * @return
	 */
	private static int getOnlineTimeRange(int onlineTime){
		if(onlineTime <= 0){
			return 0;
		}
		if(onlineTime > dayMaxOnlineTime){
			onlineTime = dayMaxOnlineTime;
		}
		
		int step = 0;
		if(onlineTime <= 10*60){
			step = 10; //10s
		}else if(onlineTime <= 60*60){
			step = 60; //60s
		}else{
			step = 10*60; //10m
		}
		
		return 0 == onlineTime%step ? onlineTime : onlineTime - onlineTime%step + step;
	}
	
	/**
	 * currency <= 10,  步长= 1
	 * 11 < currency <= 100,  步长= 2
	 * 101 < currency <= 500,  步长= 5
	 * currency > 500,  步长= 10
	 * 
	 * @param currency
	 * @return
	 */
	private static int getPaymentRange(float curr){
		int currency = Math.round(curr);
		if(currency <= 0){
			return 0;
		}
		if(currency > dayMaxPayment){
			currency = dayMaxPayment;
		}
		
		int step = 0;
		if(currency <= 10){
			step = 1;
		}else if(currency <= 100){
			step = 2;
		}else if(currency <= 500){
			step = 5;
		}else{
			step = 10;
		}
		
		return 0 == currency%step ? currency : currency - currency%step + step;
	}
	
	public static void main(String[] args){
		DecimalFormat df = new DecimalFormat();
		df.setMinimumIntegerDigits(4);
		df.setGroupingUsed(false);
		System.out.println(df.format(60*60*24));
		/*for(float i=485;i<=515;i++){
			System.out.println(i + " = "+df.format(getPaymentRange(i)));
		}*/
	}
}
