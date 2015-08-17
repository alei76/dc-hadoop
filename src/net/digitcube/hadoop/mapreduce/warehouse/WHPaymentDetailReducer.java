package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHDataUtil;
import net.digitcube.hadoop.model.warehouse.WHPaymentLog;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 数据仓库：付费明细表
 * 进行数据清理
 * @see WHPaymentDetailMapper
 * 
 * 
 * @author sam.xie
 * @date 2015年6月17日 下午6:04:31
 * @version 1.0
 */
public class WHPaymentDetailReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] keyFields = key.getOutFields();
		String uid = keyFields[0];
		String appId = keyFields[1];

		// 如果直接使用Iterable迭代，无法重复使用，这里使用列表保存
		List<WHPaymentLog> paymentList = new ArrayList<WHPaymentLog>();
		for (OutFieldsBaseModel val : values) {
			// 这里需要new 新对象，因为hadoop迭代使用的是同一个引用（坑！注意！）
			try {
				paymentList.add(new WHPaymentLog(val.getOutFields()));
			} catch (Exception e) {

			}
		}
		String filterResult = WHDataUtil.doFilter(appId, uid, paymentList);
		for (WHPaymentLog val : paymentList) {
			val.setCleanFlag(filterResult);// 更新最后一个清洗标识位
			key.setOutFields(val.returnArray()); // 这里使用value的值作为输出
			context.write(key, NullWritable.get());
		}

	}
}
