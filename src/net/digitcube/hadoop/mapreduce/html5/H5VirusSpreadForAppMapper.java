package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
  * 
 * 主要逻辑：
 * 
 */

public class H5VirusSpreadForAppMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String H5_PromotionAPP = arr[i++];
		String H5_DOMAIN = arr[i++];
		String H5_REF = arr[i++];
		String accountId = arr[i++];
		String parentAccountId = arr[i++];
		String totalLoginTimes = arr[i++];
		String totalOnlineTime = arr[i++];
		String totalPVs = arr[i++];
		String ipRecords = arr[i++];
		
		String[] keyFields = new String[] { 
				appId,
				platform,
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				parentAccountId
		};
		String[] valFields = new String[]{
				totalLoginTimes,
				totalOnlineTime,
				totalPVs,
				ipRecords
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
}
