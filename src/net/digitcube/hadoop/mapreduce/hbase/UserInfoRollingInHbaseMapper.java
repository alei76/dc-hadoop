package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserInfoRollingInHbaseMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	private Text mapKeyObj = new Text();
	private Text mapValueObj = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String appIdAppVersion = paraArr[0];
		String[] appInfo = appIdAppVersion.split("\\|");
		String appid = appInfo[0];
		String platformType = paraArr[1];
		String accountId = paraArr[2];
		String accountInfo = paraArr[3];

		// String[] keyFields = new String[] { appid + "|" + platformType + "|"
		// + accountId };
		// String[] valueFields = new String[] { accountInfo };
		mapKeyObj.set(appid + "|" + platformType + "|" + accountId);// .setOutFields(keyFields);
		mapValueObj.set(accountInfo);
		context.write(mapKeyObj, mapValueObj);
	}
}
