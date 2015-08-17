package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Player30DayValueAndArpuMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String accountId = arr[i++];
		String days = arr[i++];
		String currency = arr[i++];
		
		if(fileName.endsWith(Constants.SUFFIX_PLAYER_30DAYVALUE)){
			String[] keyFields = new String[]{appId, platform, channel, gameServer};
			String[] valFields = new String[]{days, currency};
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valFields);
			mapKeyObj.setSuffix(Constants.SUFFIX_PLAYER_30DAYVALUE_SUM);
			context.write(mapKeyObj, mapValueObj);
			
		}else if(fileName.endsWith(Constants.SUFFIX_PLAYER_30DAY_ARPU)){
			//不分版本、渠道和区服统计
			if(!MRConstants.ALL_GAMESERVER.equals(gameServer)){
				return;
			}
			String appIdWithoutVersion = appId.split("\\|")[0];
			
			String[] keyFields = new String[]{
					appIdWithoutVersion, 
					platform, 
					MRConstants.ALL_CHANNEL, 
					MRConstants.ALL_GAMESERVER};
			String[] valFields = new String[]{days, currency, accountId};
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(valFields);
			mapKeyObj.setSuffix(Constants.SUFFIX_PLAYER_30DAY_ARPU_SUM);
			context.write(mapKeyObj, mapValueObj);
		}
	}
}
