package net.digitcube.hadoop.mapreduce.sdk;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.SdkLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * <pre> 
 *  
 *  sdk版本升级通知
 *  key:
 *  { appID, platForm, sdkVersion};
 *  Suffix(Constants.SUFFIX_SDK_UPDATE_NOTICE);
 *  value:Null
 *  
 *  <br>
 */
public class SdkVersionUpdateHourMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {	
	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		SdkLog sdkLog = null; 
		try{
		    sdkLog = new SdkLog(arr);
		}catch(Exception e){
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(sdkLog.getAppID())){
			return;
		}
		
		String appID = sdkLog.getAppID();
		String platForm = sdkLog.getPlatform();
		String sdkVersion = sdkLog.getSdkVersion();
		int sdkVersionInt = StringUtil.convertInt(sdkVersion, 0);
		String extend_3 = sdkLog.getExtend_3();
		//过滤掉tcl的数据18
		if(StringUtil.isEmpty(sdkVersion) || sdkVersionInt == 18){
			return;
		}
		if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(extend_3)){
			extend_3 = MRConstants.INVALID_PLACE_INSTEAD_CHAR;
		}
		String[] outputKeyArr = new String[] {appID, platForm, extend_3};
		outputKey.setOutFields(outputKeyArr);
		outputKey.setSuffix(Constants.SUFFIX_SDK_VERSION_UPDATE);
		context.write(outputKey, NullWritable.get());	
	}

}
