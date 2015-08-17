package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagStayAddRmMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = paraArr[i++];
		String version = paraArr[i++];
		String platform = paraArr[i++];
		String channel = paraArr[i++];
		String gameServer = paraArr[i++];
		String accountId = paraArr[i++];
		String parentName = paraArr[i++];
		String tagName = paraArr[i++];
		String isActive = paraArr[i++];
		String isNewAdd = paraArr[i++];
		String isRemove = paraArr[i++];
		
		String[] keyFields = new String[]{
				appId,
				version,
				platform,
				channel,
				gameServer,
				parentName,
				tagName
		};
		String[] valFields = new String[]{
				isActive,
				isNewAdd,
				isRemove
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
}
