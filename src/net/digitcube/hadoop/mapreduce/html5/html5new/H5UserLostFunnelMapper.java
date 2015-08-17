package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class H5UserLostFunnelMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String appid = paraArr[0];
		String accountId = paraArr[1];
		String platform = paraArr[2];
		String promptApp = paraArr[3];
		String domain = paraArr[4];
		String refer = paraArr[5];		
		String userType = paraArr[6];
		String days = paraArr[7]; // N日前的留存			
		mapKeyObj.setOutFields(new String[] { appid,platform, promptApp,
				domain, refer,userType, days });
		context.write(mapKeyObj, one);
	}

}
