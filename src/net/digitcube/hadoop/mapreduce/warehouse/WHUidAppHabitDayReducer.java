package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPay;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 输入
 * @see WHUidAppHabitDayMapper
 * 
 * @author sam.xie
 * @date 2015年7月20日 下午2:23:33
 * @version 1.0
 */
public class WHUidAppHabitDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private OnlineAndPay total = new OnlineAndPay();
	private OnlineAndPay item = new OnlineAndPay();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		total.clear(); // 数据清零
		item.clear();
		int loginMark = 0;
		int payMark = 0;
		int loginDays = 0;
		int payDays = 0;
		for (OutFieldsBaseModel val : values) {
			item.clear();
			item.setParams(0, val.getOutFields());
			loginMark = loginMark | item.getLoginDays(); // 注意，在map输出中loginDays标识loginMark
			payMark = payMark | item.getPayDays();

			total.mergeDay(item); // 合并累加
		}

		for (int i = 0; i < 30; i++) {
			loginDays += ((loginMark >> i) & 1) == 1 ? 1 : 0;
			payDays += ((payMark >> i) & 1) == 1 ? 1 : 0;
		}
		total.setLoginDays(loginDays);
		total.setPayDays(payDays);

		valFields.setOutFields(total.toArray());
		context.write(key, valFields);
	}
}
