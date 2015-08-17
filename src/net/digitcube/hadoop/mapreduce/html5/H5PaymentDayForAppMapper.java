package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *	输出：appId, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, totalCurrency, totalPaytimes, totalPlayerNums
 */
public class H5PaymentDayForAppMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = paymentArr[i++];
		String platform = paymentArr[i++];
		String H5_PromotionAPP = paymentArr[i++];
		String H5_DOMAIN = paymentArr[i++];
		String H5_REF = paymentArr[i++];
		String accountId = paymentArr[i++];
		String totalCurrency = paymentArr[i++];
		String totalPaytimes = paymentArr[i++];
		
		String[] keyFields = new String[] {
				appId,
				platform,
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
		};
		mapKeyObj.setOutFields(keyFields);

		String[] valueFields = new String[]{
				totalCurrency,
				totalPaytimes
		};
		mapValueObj.setOutFields(valueFields);

		context.write(mapKeyObj, mapValueObj);
	}
}
