package test.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PayIntervalTestMapper extends
		Mapper<LongWritable, Text, Text, OutFieldsBaseModel> {

	private Text mapKeyObj = new Text();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String accountId = paymentArr[0]; 

		mapKeyObj.set(accountId);
		mapValueObj.setOutFields(paymentArr);

		context.write(mapKeyObj, mapValueObj);
	}
}
