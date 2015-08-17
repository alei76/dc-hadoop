package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午3:43:17 @copyrigt www.digitcube.net
 * 
 *          输入:<br/>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution
 *          ,opersystem,brand,nettype,country,province,operators,onlinetime
 *          ,CurrencyAmount(当日总付费),TotalPayTimes(当日付费次数)<br/>
 *          输出：<br/>
 *          key:appid,platform<br/>
 *          value: CurrencyAmount,TotalPayTimes
 * 
 */

public class PaymentDayForAppMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	public final static int Index_APPID = 0;
	public final static int Index_PLATFORM = 1;
	public final static int Index_CHANNEL = 3;
	public final static int Index_GAMESERVER = 7;
	public final static int Index_CurrencyAmount = 15;
	public final static int Index_TotalPayTimes = 16;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = new String[] { paymentArr[Index_APPID],
											paymentArr[Index_PLATFORM],
											paymentArr[Index_CHANNEL],
											paymentArr[Index_GAMESERVER]};

		mapKeyObj.setOutFields(keyFields);

		String[] valueFields = new String[]{paymentArr[Index_CurrencyAmount],
											paymentArr[Index_TotalPayTimes] };

		mapValueObj.setOutFields(valueFields);

		context.write(mapKeyObj, mapValueObj);
	}
}
