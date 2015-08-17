package net.digitcube.hadoop.mapreduce.plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * <pre>
 * 虚拟币流水
 * @author Ivan          <br>
 * @date 2015年2月11日 下午2:59:34 <br>
 * @version 1.0
 * <br>
 */
public class ItemReportReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private Map<String, Long> currencyMap = new HashMap<String, Long>();
	private OutFieldsBaseModel valueObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		currencyMap.clear();
		long itemNum = 0;
		// 合并原因和数量
		for (OutFieldsBaseModel value : values) {
			String[] arr = value.getOutFields();
			if (arr.length != 3) {
				continue;
			}
			try {
				String currencyType = arr[0];
				long virtualCurrencyNum = StringUtil.convertLong(arr[1], 0);
				long itemCnt = StringUtil.convertLong(arr[2], 0);
				itemNum += itemCnt;
				Long currencyNum = currencyMap.get(currencyType);
				if (currencyNum == null) {
					currencyMap.put(currencyType, virtualCurrencyNum);
				} else {
					currencyMap.put(currencyType, virtualCurrencyNum + currencyNum);
				}
			} catch (Exception e) {
				// print sth.
			}
		}
		valueObj.setOutFields(new String[] { StringUtil.getJsonStr(currencyMap), itemNum + "" });
		context.write(key, valueObj);
	}
}