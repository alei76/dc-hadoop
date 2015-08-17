package net.digitcube.hadoop.mapreduce.userroll.week;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年8月1日 下午2:27:17 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserWeekLostFunnelMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String appid = paraArr[0];
		String platform = paraArr[1];
		String channel = paraArr[2];
		String gameRegion = paraArr[3];
		String userType = paraArr[4];
		String weeks = paraArr[5]; // N周前的留存
		int loginTimes = StringUtil.convertInt(paraArr[6], 0);
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
		if (loginTimes > 0) { // 至少登陆1次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, weeks, "1" });
			context.write(mapKeyObj, one);
		}
		if (loginTimes > 1) {// 至少登陆2次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, weeks, "2" });
			context.write(mapKeyObj, one);
		}
		if (loginTimes > 2) { // 至少登陆3次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, weeks, "3" });
			context.write(mapKeyObj, one);
		}

	}
}
