package net.digitcube.hadoop.mapreduce.oss;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OSS;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月22日 下午8:30:48 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class OSSMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OSS oss = new OSS(paraArr);
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel(new String[] {
				oss.getAppID(), oss.getPlatform(), oss.getMonitorName(),
				oss.getCallerDesc1() });
		OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel(new String[] {
				oss.getCost(), oss.getSuccess(), oss.getFailtimes() });
		context.write(mapKeyObj, mapValueObj);
	}
}