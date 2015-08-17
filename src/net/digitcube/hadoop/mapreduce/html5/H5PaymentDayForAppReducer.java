package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5PaymentDayForAppReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel reduceValueObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		float totalCurrency = 0;
		int totalPaytimes = 0;
		int totalPlayerNums = 0;
		for (OutFieldsBaseModel val : values) {
			
			//每次循环都视为增加一个充值玩家
			totalPlayerNums++;
			totalCurrency += StringUtil.convertFloat(val.getOutFields()[0], 0);
			totalPaytimes += StringUtil.convertInt(val.getOutFields()[1], 0);
		}
		String[] outValue = new String[] { 
				totalCurrency + "",
				totalPaytimes + "",
				totalPlayerNums + ""
		};
		
		reduceValueObj.setOutFields(outValue);
		key.setSuffix(Constants.SUFFIX_PAYMENT_DAY_APP);
		context.write(key, reduceValueObj);
	}

}
