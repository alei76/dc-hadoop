package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.Pair;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * <pre>
 * 资源位点击/曝光
 * 
 * -----------------------------输入-----------------------------
 * 取自定义事件日志
 * 1.资源位曝光事件_DESelf_Channel_RL_Show
 * 2.资源位点击事件_DESelf_Channel_RL_Click
 * 
 * -----------------------------输出-----------------------------
 * 1.资源位曝光量
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constants.SUFFIX_CHANNEL_RL_SHOW
 * Value:			one
 * 
 * 2.资源位点击量
 * Key:				appID,appVersion,channel,country,province,rlId
 * Key.Suffix		Constant.SUFFIX_CHANNEL_RL_CLICK
 * Value:			one
 * 
 * 
 * @author sam.xie
 * @date 2015年3月3日 下午7:15:36
 * @version 1.0
 */
public class CHResourceLocationMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog2(arr);

		} catch (Exception e) {
			return;
		}
		String appID = eventLog.getAppID();
		String appVersion = eventLog.getAppVersion();
		String channel = eventLog.getChannel();
		String country = eventLog.getExtendValue(ExtendKey.CNTY);
		String province = eventLog.getExtendValue(ExtendKey.PROV);

		String eventAttr = eventLog.getEventAttr();

		if (Constants.DESELF_CHANNEL_RL_SHOW.equals(eventLog.getEventId())) {
			// 资源位曝光
			List<Pair<String, String>> resPairs = parseResPair(eventAttr);
			outputKey.setSuffix(Constants.SUFFIX_CHANNEL_RL_SHOW);
			for (Pair<String, String> item : resPairs) {
				String[] outFields = new String[] { appID, appVersion, channel, country, province, item.getFirst() };
				outputKey.setOutFields(outFields);
				context.write(outputKey, one);
			}
		} else if (Constants.DESELF_CHANNEL_RL_CLICK.equals(eventLog.getEventId())) {
			// 资源位点击
			String rlId = eventLog.getAttrMap().get(AttrKey.RLID);
			String resId = eventLog.getAttrMap().get(AttrKey.RESID);
			// update by sam
			// 与高境确认，resId非必须
//			if (StringUtils.isBlank(rlId) || StringUtils.isBlank(resId)) {
//				return;
//			}
			String[] outFields = new String[] { appID, appVersion, channel, country, province, rlId };
			outputKey.setSuffix(Constants.SUFFIX_CHANNEL_RL_CLICK);
			outputKey.setOutFields(outFields);
			context.write(outputKey, one);
		}
	}

	/**
	 * <pre>
	 * 解析简单的Json数组，如[{"rlId":"rlid1","resId":"resId1"},{"rlId":"rlid2","resId":"resId2"}]
	 * @author sam.xie
	 * @date 2015年3月5日 下午3:17:23
	 * @param resPair
	 * @return
	 */
	public List<Pair<String, String>> parseResPair(String resPair) {
		List<Pair<String, String>> pairList = new ArrayList<Pair<String, String>>();
		if (StringUtils.isBlank(resPair)) {
			return pairList;
		}
		try {
			JsonParser jsonParser = new JsonParser();
			JsonArray jsonArray = (JsonArray) jsonParser.parse(resPair);
			for (JsonElement ele : jsonArray.getAsJsonArray()) {
				if (ele.isJsonObject()) { // 如：{"rlId":"rlid1","resId":"resId1"}
					Pair<String, String> pair = new Pair<String, String>("-", "-"); // 默认用短横杠占位
					pairList.add(pair);

					for (Entry<String, JsonElement> entry : ele.getAsJsonObject().entrySet()) {
						String key = entry.getKey();
						String value = entry.getValue().getAsString();
						if (AttrKey.RLID.equals(key)) { // value1设置rlId
							pair.setFirst(value);
						} else if (AttrKey.RESID.equals(key)) { // value2设置resId
							pair.setSecond(value);
						}
					}
				}
			}

		} catch (Exception e) {
			// logger.error("Json Syntax Error");

		}
		return pairList;
	}

}
