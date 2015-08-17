package net.digitcube.hadoop.mapreduce.channel.week;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class CHWeekUserHabitsReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {

		int totalUser = 0;
		int totalLoginTimes = 0;
		int totalOnlineTime = 0;
		
		for (OutFieldsBaseModel val : values) {
			int loginTimes = StringUtil.convertInt(val.getOutFields()[0], 0);
			int onlineTime = StringUtil.convertInt(val.getOutFields()[1], 0);
			totalUser++;
			totalLoginTimes += loginTimes;
			totalOnlineTime += onlineTime;

		}

		key.setSuffix(Constants.SUFFIX_CHANNEL_WEEK_HABITS);
		valObj.setOutFields(new String[]{
				totalUser+"",
				totalLoginTimes+"",
				totalOnlineTime+""
		});
		context.write(key, valObj);
	}
}
