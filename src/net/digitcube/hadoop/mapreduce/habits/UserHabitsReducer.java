package net.digitcube.hadoop.mapreduce.habits;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月22日 下午8:26:58 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserHabitsReducer
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

		int totalOnlineTime = 0;
		int totalLoginTimes = 0;
		int totalUser = 0;
		String tag = null;
		for (OutFieldsBaseModel val : values) {
			String[] valArray = val.getOutFields();
			if (tag == null) {
				tag = valArray[0];
			}
			if ("A".equals(tag)) { // 统计在线时长
				totalUser++;
				totalLoginTimes += StringUtil.convertInt(valArray[1], 0);
				totalOnlineTime += StringUtil.convertInt(valArray[2], 0);
			} else if ("B".equals(tag)) {
				totalUser++;
			}

		}
		if ("A".equals(tag)) { // 统计在线时长
			if (totalOnlineTime != 0 && totalLoginTimes != 0 && totalUser != 0) {
				float avgOnlineTime = totalOnlineTime / totalUser;
				float avgLoginTimes = totalLoginTimes / totalUser;
				key.setSuffix(Constants.SUFFIX_USER_HABITS_APP);
				context.write(key, new OutFieldsBaseModel(
						new String[] { totalLoginTimes + "",
								totalOnlineTime + "", avgLoginTimes + "",
								avgOnlineTime + "", totalUser + "" }));
			}
		} else if ("B".equals(tag)) { // 已玩天数,在线时长,等级,日游戏次数
			key.setSuffix(Constants.SUFFIX_USER_LAYOUT_DAYS);
			context.write(key, new OutFieldsBaseModel(new String[] { totalUser
					+ "" }));
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}
}
