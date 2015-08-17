package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PaymentStatDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {

		//A. 每个 APP 付费统计
		if(Constants.SUFFIX_PAYMENT_DAY_APP.equals(key.getSuffix()) 
				|| Constants.SUFFIX_NEW_PLAYER_PAY_APP.equals(key.getSuffix())){
			float totalCurrencyAmount = 0;
			int totalPaytimes = 0;
			int totalPlayerNums = 0;
			for (OutFieldsBaseModel val : values) {
				//每次循环都视为增加一个充值玩家
				totalPlayerNums++;
				float currencyAmount = StringUtil.convertFloat(val.getOutFields()[0], 0);
				totalCurrencyAmount += currencyAmount;
				totalPaytimes += StringUtil.convertInt(val.getOutFields()[1], 0);
			}
			
			String[] keyFields = new String[key.getOutFields().length + 3];
			System.arraycopy(key.getOutFields(), 0, keyFields, 0, key.getOutFields().length);
			keyFields[keyFields.length - 3] = totalCurrencyAmount+"";
			keyFields[keyFields.length - 2] = totalPaytimes+"";
			keyFields[keyFields.length - 1] = totalPlayerNums+"";
			
			//keyObj.setSuffix(Constants.SUFFIX_PAYMENT_DAY_APP);
			keyObj.setSuffix(key.getSuffix());
			keyObj.setOutFields(keyFields);
			context.write(keyObj, NullWritable.get());
			
		//B. 付费等级统计
		}else if(Constants.SUFFIX_PAYMENT_LAYOUT_ON_LEVEL.equals(key.getSuffix())){
			// 对同一个付费等级所有玩家的进行付费人次和付费金额的统计
			keyObj.setSuffix(Constants.SUFFIX_PAYMENT_LAYOUT_ON_LEVEL);
			int totalPaytimes = 0;
			float totalCurrencyAmount = 0;
			for (OutFieldsBaseModel val : values) {
				String amount = val.getOutFields()[0];
				float currencyAmount = StringUtil.convertFloat(amount, 0);
				totalCurrencyAmount += currencyAmount;
				totalPaytimes++;
			}
			
			// 等级付费金额
			String[] keyFields = key.getOutFields();
			String[] payLeCurrency = new String[]{
					keyFields[0],	// appId
					keyFields[1],	// platform
					keyFields[2],	// channel
					keyFields[3],	// gameServer
					Constants.DIMENSION_PAY_LEVEL_CURRENCY,	// 维度：等级付费金额
					keyFields[4],	// 维度值：那个等级
					""+totalCurrencyAmount	// 该级别的付费总金额
			};
			keyObj.setOutFields(payLeCurrency);
			context.write(keyObj, NullWritable.get());
			
			// 等级付费人次
			String[] payLeTimes = new String[]{
					keyFields[0],	// appId
					keyFields[1],	// platform
					keyFields[2],	// channel
					keyFields[3],	// gameServer
					Constants.DIMENSION_PAY_LEVEL_TIMES,	// 维度：等级付费金额
					keyFields[4],	// 维度值：那个等级
					""+totalPaytimes	// 该级别的付费总人次（次数）
			};
			keyObj.setOutFields(payLeTimes);
			context.write(keyObj, NullWritable.get());
			
		//C. 收入分布统计
		}else if(Constants.SUFFIX_LAYOUT_ON_INCOME.equals(key.getSuffix())){
			//收入分布
			float total = 0;
			for (OutFieldsBaseModel val : values) {
				total += StringUtil.convertFloat(val.getOutFields()[0], 0);
			}
			
			String[] keyFields = new String[key.getOutFields().length + 1];
			System.arraycopy(key.getOutFields(), 0, keyFields, 0, key.getOutFields().length);
			keyFields[keyFields.length - 1] = total + "";
			
			keyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_INCOME);
			keyObj.setOutFields(keyFields);
			context.write(keyObj, NullWritable.get());
		}
	}

}
