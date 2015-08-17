package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 资源/资源位/搜索下载统计
 * 
 * -----------------------------输入-----------------------------
 * 1.资源下载统计（Type区分资源位和搜索词）
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL
 * Value:			Type,RlID/Keyword
 * 
 * 2.资源位下载统计
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL
 * Value:			One
 * 
 * 3.搜索字下载统计
 * Key:				appID,appVersion,channel,country,province,keyword
 * Suffix:			Constants.SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL
 * Value:			One
 * 
 * 4.资源下载成功
 * Key:				appID,appVersion,channel,country,province,resId
 * Suffix:			Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT
 * Value:			One
 * 
 * -----------------------------输出-----------------------------
 * 1.资源下载汇总
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL
 * Value:			sum
 * 
 * 2.资源下载-按资源位汇总（搜索作为一个特殊资源位）
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RL
 * Value:			rlId,num
 * 
 * 3.资源下载-按关键字汇总
 * Key:				appID,appVersion,channel,country,province,resId	
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_KW
 * Value:			keyword,sum
 * 
 * 4.资源位下载
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix:		Constants.SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL
 * Value:			sum
 * 
 * 5.关键字下载
 * Key:				appID,appVersion,channel,country,province,keyword
 * Key.Suffix		Constants.SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL
 * Value:			sum
 * 
 * 6.资源下载成功
 * Key:				appID,appVersion,channel,country,province,resId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT
 * Value:			sum
 * 
 * 
 * @author sam.xie
 * @date 2015年3月4日 下午7:56:50
 * @version 1.0
 */
public class CHResourceDownloadReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private Map<String, Integer> mapRL = new HashMap<String, Integer>();
	private Map<String, Integer> mapKW = new HashMap<String, Integer>();
	private static final String KW_AS_RL = "KW_AS_RL"; // 关键字作为一种特殊的资源位

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String suffix = key.getSuffix();
		if (Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_TOTAL.equals(suffix)) {
			int resTotal = 0;
			String[] keyArr = key.getOutFields();
			mapRL.clear();
			mapKW.clear();
			// 需要进一步按资源位/关键字分组
			for (OutFieldsBaseModel item : values) {
				String[] fields = item.getOutFields();
				String type = fields[0];
				String val = fields[1]; // val可能为rlId/keyword
				if ("RL".equals(type)) { // RL表示资源位
					if (null == mapRL.get(val)) {
						mapRL.put(val, 1);
					} else {
						mapRL.put(val, mapRL.get(val) + 1); // 资源位计数自增
					}
				} else if ("KW".equals(type)) { // KW表示关键字
					if (null == mapKW.get(val)) {
						mapKW.put(val, 1);
					} else {
						mapKW.put(val, mapKW.get(val) + 1); // 关键字计数自增
					}
					// 所有KW作为一种特殊的资源位保存
					if (null == mapRL.get(KW_AS_RL)) {
						mapRL.put(KW_AS_RL, 1);
					} else {
						mapRL.put(KW_AS_RL, mapRL.get(KW_AS_RL) + 1);
					}
				}
				resTotal += 1; // 资源计数自增
			}
			// 资源下载按资源位分布
			for (Entry<String, Integer> entry : mapRL.entrySet()) {
				String rl = entry.getKey();
				String num = entry.getValue().toString();
				OutFieldsBaseModel keyRL = new OutFieldsBaseModel(keyArr);
				OutFieldsBaseModel valueRL = new OutFieldsBaseModel(new String[] { rl, num });
				keyRL.setSuffix(Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RL);
				context.write(keyRL, valueRL);
			}

			// 资源下载按关键字分布
			for (Entry<String, Integer> entry : mapKW.entrySet()) {
				String keyword = entry.getKey();
				String num = entry.getValue().toString();
				OutFieldsBaseModel keyKW = new OutFieldsBaseModel(keyArr);
				OutFieldsBaseModel valueRL = new OutFieldsBaseModel(new String[] { keyword, num });
				keyKW.setSuffix(Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_KW);
				context.write(keyKW, valueRL);
			}
			context.write(key, new OutFieldsBaseModel(new String[] { resTotal + "" }));

		} else if (Constants.SUFFIX_CHANNEL_RL_DOWNLOAD_TOTAL.equals(suffix)
				|| Constants.SUFFIX_CHANNEL_KW_DOWNLOAD_TOTAL.equals(suffix)
				|| Constants.SUFFIX_CHANNEL_RES_DOWNLOAD_RESULT.equals(suffix)) { // 资源位/关键字直接计数即可
			int tmp = 0;
			for (OutFieldsBaseModel item : values) {
				tmp += 1;
			}
			context.write(key, new OutFieldsBaseModel(new String[] { tmp + "" }));
		}
	}
}
