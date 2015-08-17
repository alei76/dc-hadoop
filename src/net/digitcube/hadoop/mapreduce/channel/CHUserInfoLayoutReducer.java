package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class CHUserInfoLayoutReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		if(Constants.SUFFIX_CHANNEL_ONLINE_SUM.equals(key.getSuffix())){
			int totalPlayerNum = 0;
			int totalLoginTimes = 0;
			int totalOnlineTime = 0;
			for(OutFieldsBaseModel val : values){
				totalPlayerNum++;
				totalLoginTimes += StringUtil.convertInt(val.getOutFields()[0], 0);
				totalOnlineTime += StringUtil.convertInt(val.getOutFields()[1], 0);
			}
			
			String[] valFields = new String[]{
					totalPlayerNum+"",
					totalLoginTimes+"",
					totalOnlineTime+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
			
		}else{
			int total = 0;
			for(OutFieldsBaseModel val : values){
				total++;
			}
			String[] valFields = new String[]{
					total+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}
	}
}
