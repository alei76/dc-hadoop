package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午5:05:26 @copyrigt www.digitcube.net<br/>
 *          输入：<br/>
 *          key:appid,platform,channel,gameServer<br/>
 *          value: CurrencyAmount,TotalPayTimes<br/>
 *          输出：<br/>
 *          appid,platform,channel,gameServer,CurrencyAmount(总付费),TotalPayTimes(总付费次数),TotalPlayerNums(总充值人数)<br/>
 * 
 */

public class PaymentDayForAppReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel reduceValueObj = new OutFieldsBaseModel();
	private final static int Index_CurrencyAmount = 0;
	private final static int Index_TotalPayTimes = 1;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// do some setup before map
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		float totalCurrencyAmount = 0;
		int totalPaytimes = 0;
		int totalPlayerNums = 0;
		String[] defaultLastValue = null;
		for (OutFieldsBaseModel val : values) {
			
			//每次循环都视为增加一个充值玩家
			totalPlayerNums++;
			
			defaultLastValue = val.getOutFields();

			float currencyAmount = StringUtil.convertFloat(
					defaultLastValue[Index_CurrencyAmount], 0);
			totalCurrencyAmount += currencyAmount;
			totalPaytimes += StringUtil.convertInt(
					defaultLastValue[Index_TotalPayTimes], 0);
		}
		String[] outValue = new String[] { totalCurrencyAmount + "",
										   totalPaytimes + "",
										   totalPlayerNums + ""};
		
		reduceValueObj.setOutFields(outValue);
		key.setSuffix(Constants.SUFFIX_PAYMENT_DAY_APP);
		context.write(key, reduceValueObj);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
