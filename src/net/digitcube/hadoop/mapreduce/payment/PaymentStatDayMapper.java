package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PaymentStatDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentDayLog paymentDayLog = new PaymentDayLog(array);
		
		//A. 统计收入、付费人数、付费次数
		writePaymentForApp(context, paymentDayLog);
		
		//B. 统计付费等级
		writeLayoutOnLevel(context, paymentDayLog);
		
		//C. 统计收入分布
		writeLayoutOnIncome(context, paymentDayLog);
	}
	
	private void writePaymentForApp(Context context, PaymentDayLog paymentDayLog) throws IOException, InterruptedException{
		String[] keyFields = new String[] { 
				paymentDayLog.getAppID(),
				paymentDayLog.getPlatform(),
				paymentDayLog.getExtend().getChannel(),
				paymentDayLog.getExtend().getGameServer()
		};
		mapKeyObj.setOutFields(keyFields);
		
		String[] valueFields = new String[]{
				paymentDayLog.getCurrencyAmount()+"",
				paymentDayLog.getTotalPayTimes()+""
		};
		mapValueObj.setOutFields(valueFields);
		
		mapKeyObj.setSuffix(Constants.SUFFIX_PAYMENT_DAY_APP);
		context.write(mapKeyObj, mapValueObj);
		
		// 20180827 : 付费统计中分出新增玩家和活跃玩家
		// 由于 paymentDayLog 中没有多余的字段标识新增玩家
		// 而下游 MR 中没有用到 Resolution, 所以新增玩家标识暂存在 Resolution 字段里
		String playerType = paymentDayLog.getExtend().getResolution();
		if(Constants.PLAYER_TYPE_NEWADD.equals(playerType)){
			String[] keyFieldsNew = new String[] { 
					paymentDayLog.getAppID(),
					paymentDayLog.getPlatform(),
					paymentDayLog.getExtend().getChannel(),
					paymentDayLog.getExtend().getGameServer(),
					Constants.PLAYER_TYPE_NEWADD
			};
			mapKeyObj.setOutFields(keyFieldsNew);
			
			String[] valueFieldsNew = new String[]{
					paymentDayLog.getCurrencyAmount()+"",
					paymentDayLog.getTotalPayTimes()+""
			};
			mapValueObj.setOutFields(valueFieldsNew);
			
			mapKeyObj.setSuffix(Constants.SUFFIX_NEW_PLAYER_PAY_APP);
			context.write(mapKeyObj, mapValueObj);
		}
	}
	
	private void writeLayoutOnLevel(Context context, PaymentDayLog paymentDayLog) throws IOException, InterruptedException{
		//Added at 20140606 : iOS 渠道修正
		/*if(MRConstants.PLATFORM_iOS_STR.equals(paymentLog.getPlatform())){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(paymentLog.getAppID(), 
																paymentLog.getUID(), 
																paymentLog.getPlatform(), 
																paymentLog.getChannel());
			paymentLog.setChannel(reviseChannel);
		}*/
		
		String[] payRecords = paymentDayLog.getPayRecords().split(",");
		for (String record : payRecords) {
			String[] arr = record.split(":");
			if(arr.length < 3){
				continue;
			}
			
			//payTime:currencyAmount:level
			String payTime = arr[0];
			String currencyAmount = arr[1];
			String level = arr[2];
			
			String[] keyFields = new String[]{
					paymentDayLog.getAppID(),
					paymentDayLog.getPlatform(),
					paymentDayLog.getExtend().getChannel(),
					paymentDayLog.getExtend().getGameServer(),
					"0".equals(level) ? "1" : level
			};
			
			String[] valFields = new String[]{
					currencyAmount
			};
			
			// set data flag for payment layout on level
			mapKeyObj.setSuffix(Constants.SUFFIX_PAYMENT_LAYOUT_ON_LEVEL);
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valFields);
			
			context.write(mapKeyObj, mapValueObj);
		}
	}
	
	private void writeLayoutOnIncome(Context context, PaymentDayLog paymentDayLog) throws IOException, InterruptedException{
		
		String[] valFields = new String[]{
				paymentDayLog.getCurrencyAmount() + ""
		};
		mapValueObj.setOutFields(valFields);
		
		//地区
		String[] areaArr = new String[]{paymentDayLog.getAppID(), 
									 paymentDayLog.getPlatform(), 
									 paymentDayLog.getExtend().getChannel(),
									 paymentDayLog.getExtend().getGameServer(),
									 Constants.DIMENSION_PLAYER_AREA, 
									 paymentDayLog.getExtend().getProvince()};
		String[] cntryArr = new String[]{paymentDayLog.getAppID(), 
									 paymentDayLog.getPlatform(), 
									 paymentDayLog.getExtend().getChannel(),
									 paymentDayLog.getExtend().getGameServer(),
									 Constants.DIMENSION_PLAYER_COUNTRY, 
									 paymentDayLog.getExtend().getCountry()};
		
		//性别
		String[] genderArr = new String[] { paymentDayLog.getAppID(), 
										 paymentDayLog.getPlatform(), 
										 paymentDayLog.getExtend().getChannel(),
										 paymentDayLog.getExtend().getGameServer(),
										 Constants.DIMENSION_PLAYER_GENDER, 
										 paymentDayLog.getExtend().getGender()}; 
		
		//年龄
		int age = StringUtil.convertInt(paymentDayLog.getExtend().getAge(), 0);
		//String ageRange = EnumConstants.Age.getAgeRange(age);
		int ageRange = EnumConstants.getRangeTop4Age(age);
		String[] ageArr = new String[] { paymentDayLog.getAppID(), 
										 paymentDayLog.getPlatform(), 
										 paymentDayLog.getExtend().getChannel(),
										 paymentDayLog.getExtend().getGameServer(),
									   	 Constants.DIMENSION_PLAYER_AGE, 
									   	 ""+ageRange};
		
		//省份
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_INCOME);
		mapKeyObj.setOutFields(areaArr);
		context.write(mapKeyObj, mapValueObj);
		//国家
		mapKeyObj.setOutFields(cntryArr);
		context.write(mapKeyObj, mapValueObj);
		//性别
		mapKeyObj.setOutFields(genderArr);
		context.write(mapKeyObj, mapValueObj);
		//年龄
		mapKeyObj.setOutFields(ageArr);
		context.write(mapKeyObj, mapValueObj);
		
		//人数分布，每条记录算一人
		mapValueObj.setOutFields(new String[]{"1"});
		//付费次数区间人数分布
		int payTimesRange = paymentDayLog.getTotalPayTimes();
		payTimesRange = EnumConstants.getDayPayTimesRange(payTimesRange);
		String[] payTimesArr = new String[] { paymentDayLog.getAppID(), 
										 paymentDayLog.getPlatform(), 
										 paymentDayLog.getExtend().getChannel(),
										 paymentDayLog.getExtend().getGameServer(),
									   	 Constants.DIMENSION_PLAYER_PayTimes, 
									   	 ""+payTimesRange};
		mapKeyObj.setOutFields(payTimesArr);
		context.write(mapKeyObj, mapValueObj);
		//付费金额区间人数分布
		int payAmountRange = new Float(paymentDayLog.getCurrencyAmount()).intValue();
		payAmountRange = EnumConstants.getDayPayAmountRange2(payAmountRange);
		String[] payAmountArr = new String[] { paymentDayLog.getAppID(), 
										 paymentDayLog.getPlatform(), 
										 paymentDayLog.getExtend().getChannel(),
										 paymentDayLog.getExtend().getGameServer(),
									   	 Constants.DIMENSION_PLAYER_PayAmount, 
									   	 ""+payAmountRange};
		mapKeyObj.setOutFields(payAmountArr);
		context.write(mapKeyObj, mapValueObj);
	}
}
