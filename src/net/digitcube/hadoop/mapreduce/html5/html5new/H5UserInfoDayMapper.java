package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5UserInfoDayLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class H5UserInfoDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] userInfoArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog userInfoLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			userInfoLog = new UserInfoLog(userInfoArr);
		}catch(Exception e){
			return;
		}		
		
		if(!FieldValidationUtil.validateAppIdLength(userInfoLog.getAppID()) 
				|| userInfoLog.isReferDomainEmpty() 
				|| StringUtil.isEmpty(userInfoLog.getH5ParentAccId())){
			return;
		}		
		
		// 去重
		String[] keyFields = new String[] { userInfoLog.getAppID(),userInfoLog.getAccountID(),
				userInfoLog.getPlatform(),userInfoLog.getH5PromotionApp(),
				userInfoLog.getH5Domain(),userInfoLog.getH5Refer() };
		
		H5UserInfoDayLog h5UserInfoDayLog = new H5UserInfoDayLog();
		setH5UserInfoDayLog(userInfoLog,h5UserInfoDayLog);			
		String[] valFields = h5UserInfoDayLog.toStringArray();		
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);		
		
	}
	
	// 设置H5UserInfoDayLog
	private void setH5UserInfoDayLog(UserInfoLog userInfoLog,H5UserInfoDayLog h5UserInfoDayLog){
		h5UserInfoDayLog.setAppId(userInfoLog.getAppID());
		h5UserInfoDayLog.setAccountId(userInfoLog.getAccountID());
		h5UserInfoDayLog.setPlatform(userInfoLog.getPlatform());
		h5UserInfoDayLog.setH5app(userInfoLog.getH5PromotionApp());
		h5UserInfoDayLog.setH5domain(userInfoLog.getH5Domain());
		h5UserInfoDayLog.setH5ref(userInfoLog.getH5Refer());
		h5UserInfoDayLog.setH5crtime(StringUtil.convertInt(userInfoLog.getH5CRtTime(),0));
		h5UserInfoDayLog.setUid(userInfoLog.getUID());
		h5UserInfoDayLog.setAccountType(userInfoLog.getAccountType());
		h5UserInfoDayLog.setGender(userInfoLog.getGender());
		h5UserInfoDayLog.setAge(userInfoLog.getAge());
		h5UserInfoDayLog.setResolution(userInfoLog.getResolution());
		h5UserInfoDayLog.setOpSystem(userInfoLog.getOperSystem());
		h5UserInfoDayLog.setBrand(userInfoLog.getBrand());
		h5UserInfoDayLog.setNetType(userInfoLog.getNetType());
		h5UserInfoDayLog.setCountry(userInfoLog.getCountry());
		h5UserInfoDayLog.setProvince(userInfoLog.getProvince());
		h5UserInfoDayLog.setOperators(userInfoLog.getOperators());		
		h5UserInfoDayLog.setParentAccoutId(userInfoLog.getH5ParentAccId());
	}

}
