package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 输入：@CHPageTreeForLoginTimeReducer
 * appId,appVersion,channel,country,province,uid,loginTime,[page1:duration1,page2:dur,...,pageN:dur]
 * 
 * 输出：
 * appId,appVersion,channel,country,province,[page1:duration1,page2:dur,...,pageN:dur]
 * 
 */
public class CHPageTreeSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private Text valObj = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		valObj.clear();

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String appVersion = arr[i++];
		String channel = arr[i++];
		String country = arr[i++];
		String province = arr[i++];
		String uid = arr[i++];
		String loginTime = arr[i++];
		String pages = arr[i++];

		String[] keyFields = new String[] { appId, appVersion, channel, country, province };

		keyObj.setOutFields(keyFields);
		valObj.set(pages);
		context.write(keyObj, valObj);
	}
}
