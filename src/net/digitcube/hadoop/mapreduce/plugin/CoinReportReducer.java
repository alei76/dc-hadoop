package net.digitcube.hadoop.mapreduce.plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.Text;
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
public class CoinReportReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> {

	private Map<String, Long> reasonMap = new HashMap<String, Long>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		reasonMap.clear();
		// 合并原因和数量
		for (OutFieldsBaseModel value : values) {
			String[] arr = value.getOutFields();
			if (arr.length != 2) {
				continue;
			}
			try {
				String reason = arr[0];
				long num = StringUtil.convertLong(arr[1], 0);
				Long reasonNum = reasonMap.get(reason);
				if (reasonNum == null) {
					reasonMap.put(reason, num);
				} else {
					reasonMap.put(reason, reasonNum + num);
				}
			} catch (Exception e) {
				// print sth.
			}
		}
		context.write(key, new Text(StringUtil.getJsonStr(reasonMap)));
	}
}
