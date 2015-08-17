package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入： 流失用户最后使用页面和功能
 * 
 * 主要逻辑： 统计 14/30 流失用户最后使用页面和功能
 * 
 * 输出：
 * appId, platform, channel, gameServer, lostType(14/30), name(pageName|moduleName), totalCount
 */
public class LostUserLastUsePgAndMdMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String accountId = arr[i++];
		String lostType = arr[i++];
		String lastViewPage = arr[i++];
		String lastUseModule = arr[i++];
		
		//最后使用页面
		if(!MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(lastViewPage)){
			String[] keyFields = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					lostType, //outflow type
					Constants.DIMENSION_APP_PAGE, //dimType
					lastViewPage //vkey
			};
			keyObj.setOutFields(keyFields);
			context.write(keyObj, valObj);
		}
		
		//最后使用功能
		if(!MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(lastUseModule)){
			String[] keyFields = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					lostType, //outflow type
					Constants.DIMENSION_APP_FUNCTION, //dimType
					lastUseModule // vkey
			};
			keyObj.setOutFields(keyFields);
			context.write(keyObj, valObj);
		}
	}
}
