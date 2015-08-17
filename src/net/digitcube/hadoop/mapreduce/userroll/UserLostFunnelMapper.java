package net.digitcube.hadoop.mapreduce.userroll;

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
 * @version 1.0 2013年7月22日 下午4:45:21 <br>
 * @copyrigt www.digitcube.net <br>
 *           输入：APPID PlatForm first9Days<br>
 *           输出:key : APPID PlatForm yyyyMM(dd-9 ~ dd) N(1-9) , value: 1
 */

public class UserLostFunnelMapper extends
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
		String days = paraArr[5]; // N日前的留存
		int loginTimes = StringUtil.convertInt(paraArr[6], 0);
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
		if (loginTimes > 0) { // 至少登陆1次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, days, "1" });
			context.write(mapKeyObj, one);
		}
		if (loginTimes > 1) {// 至少登陆2次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, days, "2" });
			context.write(mapKeyObj, one);
		}
		if (loginTimes > 2) { // 至少登陆3次
			mapKeyObj.setOutFields(new String[] { appid, platform, channel,
					gameRegion, userType, days, "3" });
			context.write(mapKeyObj, one);
		}

	}

}
