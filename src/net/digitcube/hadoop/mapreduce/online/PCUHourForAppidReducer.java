package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午2:21:30 @copyrigt www.digitcube.net <br>
 * <br>
 *          输入: key = APPID,platform,gameregion 	value = ponit,onlinenum在线人数<br>
 *          输出：APPID,platform,gameregion, MAX(onlinenum),MaxOnlinePonit<br>
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class PCUHourForAppidReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		int MaxOnlineNum = 0; // 最高在线的人数
		String MaxOnlineTimePoint = ""; // 最高在线时的时间点
		for (OutFieldsBaseModel val : values) {
			String[] value = val.getOutFields();
			int onlineNum = StringUtil.convertInt(value[1], 0);
			if (onlineNum > MaxOnlineNum) {
				MaxOnlineTimePoint = value[0];
				MaxOnlineNum = onlineNum;
			}
		}
		OutFieldsBaseModel outValue = new OutFieldsBaseModel();
		outValue.setOutFields(new String[] { MaxOnlineNum + "",
				MaxOnlineTimePoint });
		key.setSuffix(Constants.SUFFIX_PCU_HOUR_APP);
		context.write(key, outValue);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
