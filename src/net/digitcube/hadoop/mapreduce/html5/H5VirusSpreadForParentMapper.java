package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
  * 
 * 主要逻辑：
 * 
 */

public class H5VirusSpreadForParentMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String FLAG_ONLINE = "OL";
	public static final String FLAG_VIRUS = "VR";
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private String fileName = "";
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		if(fileName.endsWith(Constants.SUFFIX_H5_NEW_ONLINEDAY)){		
			H5OnlineDayLog h5OnlineDayLog = null;
			try{
				h5OnlineDayLog = new H5OnlineDayLog(arr);
			}catch(Exception ex){
				return ;
			}		
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(h5OnlineDayLog.getAppId())){
				return;
			}
			
			String appId = h5OnlineDayLog.getAppId();
			String accountId = h5OnlineDayLog.getAccountId();
			String platform = h5OnlineDayLog.getPlatform();
			String H5_PromotionAPP = h5OnlineDayLog.getH5app();
			String H5_DOMAIN = h5OnlineDayLog.getH5domain();
			String H5_REF = h5OnlineDayLog.getH5ref();	
			
			//活跃玩家
			String[] keyFields = new String[] {
					appId,
					platform,
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					accountId
			};
			String[] valFields = new String[]{
					"ANY_VALUE"
			};
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(FLAG_ONLINE);
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(Constants.SUFFIX_H5_VIRUS_SPREAD_FOR_PLAYER)){
			
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
			valObj.setSuffix(FLAG_VIRUS);
			context.write(keyObj, valObj);
		}
	}
}
