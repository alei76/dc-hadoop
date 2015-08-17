package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 逻辑
 * 输入日志：付费玩家当天去重日志
 * 对每个输入，分别在 地区、性别和年龄三个维度进行统计
 * 
 * 输出：
 * key : appId, platform, channel, gameServer, 维度指标(如年龄, age), 维度值(15)
 * value : currencyAmount
 * 
 */

public class LayoutOnInComeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, FloatWritable> {
	
	private final static FloatWritable currencyAmount = new FloatWritable();
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentDayLog paymentDayLog = new PaymentDayLog(array);
		
		currencyAmount.set(paymentDayLog.getCurrencyAmount());
		
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
		
		//渠道
		//渠道统计提到上一级
		/*String[] channelArr = new String[] {paymentDayLog.getAppID(), 
										 paymentDayLog.getPlatform(), 
										 paymentDayLog.getExtend().getGameServer(),
			   						  	 Constants.DIMENSION_PLAYER_CHANNEL, 
			   						  	 paymentDayLog.getExtend().getChannel()};*/ 
		
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
		mapKeyObj.setOutFields(areaArr);
		context.write(mapKeyObj, currencyAmount);
		//国家
		mapKeyObj.setOutFields(cntryArr);
		context.write(mapKeyObj, currencyAmount);
		
		mapKeyObj.setOutFields(genderArr);
		context.write(mapKeyObj, currencyAmount);
		
		
		mapKeyObj.setOutFields(ageArr);
		context.write(mapKeyObj, currencyAmount);
	}
	
}
