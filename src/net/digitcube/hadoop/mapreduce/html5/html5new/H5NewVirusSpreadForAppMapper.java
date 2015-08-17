package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class H5NewVirusSpreadForAppMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (fileSuffix.contains(Constants.SUFFIX_H5_NEW_VIRUS_SPREAD)) {		
			String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
			int i = 0;
			String appId = arr[i++];
			String accountId = arr[i++];
			String platform = arr[i++];
			String H5_PromotionAPP = arr[i++];
			String H5_DOMAIN = arr[i++];
			String H5_REF = arr[i++];		
			String parentAccountId = arr[i++];
			String totalLoginTimes = arr[i++];
			String totalOnlineTime = arr[i++];
			String totalPVs = arr[i++];
			String ipRecords = arr[i++];
			
			String[] keyFields = new String[] { 
					appId,				
					parentAccountId
			};
			String[] valFields = new String[]{
					platform,
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					totalLoginTimes,
					totalOnlineTime,
					totalPVs,
					ipRecords
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix("VIRUS");
			context.write(keyObj, valObj);
		}else if(fileSuffix.contains(Constants.SUFFIX_H5_NEW_ONLINEDAY)){			
			String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
			int i = 0;
			String appId = arr[i++];
			String accountId = arr[i++];			
			String[] keyFields = new String[] { 
					appId,				
					accountId
			};
			String[] valFields = new String[]{
					"A"
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix("ONELINE");
			context.write(keyObj, valObj);
			
			
		}
	}
}
