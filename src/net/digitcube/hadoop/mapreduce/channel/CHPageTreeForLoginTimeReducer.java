package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 输入：@see CHPageTreeForLoginTimeMapper
 * 
 * 输出： 
 * 1.按访问时间拼接的页面路径
 * Key: 			appId,appVersion,channel,country,province,uid,loginTime
 * Key.Suffix		Constants.SUFFIX_CHANNEL_PAGETREE_4_LOGINTIME
 * Value 			[page1:duration1,page2:dur,...,pageN:dur]
 * 
 * @author sam.xie
 * @date 2015年3月20日 下午6:41:41
 * @version 1.0
 */
public class CHPageTreeForLoginTimeReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> {

	private StringBuilder sb = new StringBuilder();
	private TreeMap<String, String> viewTimeMap = new TreeMap<String, String>();
	private Text valObj = new Text();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		valObj.clear();
		viewTimeMap.clear();
		sb.delete(0, sb.length());

		for (OutFieldsBaseModel val : values) { // 按访问时间的先后顺序，将访问页面和停留时间串起来
			String viewTime = val.getOutFields()[0];
			String pageName = val.getOutFields()[1];
			String duration = val.getOutFields()[2];
			viewTimeMap.put(viewTime, pageName + ":" + duration);
		}

		Set<String> keySet = viewTimeMap.keySet();
		for (String keyStr : keySet) {
			sb.append(viewTimeMap.get(keyStr)).append(",");
		}

		valObj.set(sb.toString());

		key.setSuffix(Constants.SUFFIX_CHANNEL_PAGETREE_4_LOGINTIME);
		context.write(key, valObj);
	}
}
