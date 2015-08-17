package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5PvDayLog;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5RollingLog;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class H5UserRollingDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	
	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	// 当前输入的文件后缀
	private String fileSuffix = "";
	// 统计的数据时间
	private int statTime = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(),
				new Date());
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = null;
		String[] valueFields = null;
		String valueSuffix = "";
		if (fileSuffix.contains(Constants.SUFFIX_H5_USERROLLING)) { // 滚存日志
			H5RollingLog h5RollingLog = new H5RollingLog(scheduleTime, paraArr);
			if(!FieldValidationUtil.validateAppIdLength(h5RollingLog.getAppId())){
				return;
			}
			String[] appInfo = h5RollingLog.getAppId().split("\\|");
			keyFields = new String[] {
					appInfo[0], // 纯 appId，不带版本号					
					h5RollingLog.getAccountId()
					};
			valueFields = new String[] { "U",
					h5RollingLog.getInfoBase64(), appInfo[1] // 版本号
					};
			valueSuffix = "U";				
		}else if(fileSuffix.contains(Constants.SUFFIX_H5_NEW_ONLINEDAY)){
			H5OnlineDayLog h5OnlineDayLog = new H5OnlineDayLog(paraArr);
			String[] appInfo = h5OnlineDayLog.getAppId().split("\\|");
			String accountId = h5OnlineDayLog.getAccountId();
			keyFields = new String[] { appInfo[0],accountId};
			valueFields = paraArr;
			valueSuffix = "O";
		}else if(fileSuffix.contains(Constants.SUFFIX_H5_NEW_PV)){
			H5PvDayLog h5PvDayLog = new H5PvDayLog(paraArr);
			String[] appInfo = h5PvDayLog.getAppId().split("\\|");
			String accountId = h5PvDayLog.getAccountId();
			keyFields = new String[] { appInfo[0],accountId};
			valueFields = paraArr;
			valueSuffix = "PV";			
		}else if(fileSuffix.contains(Constants.SUFFIX_USERINFO_DAY)){
			UserInfoLog userInfoLog = new UserInfoLog(paraArr);		
			
			if(userInfoLog.isReferDomainEmpty() || StringUtil.isEmpty(userInfoLog.getH5ParentAccId())){
				return;
			}
			
			String accountId = userInfoLog.getAccountID();
			String[] appInfo = userInfoLog.getAppID().split("\\|");
			keyFields = new String[] { appInfo[0],accountId };
			valueFields = new String[] { "R",userInfoLog.getPlatform(),
					userInfoLog.getH5PromotionApp(),
					userInfoLog.getH5Domain(),
					userInfoLog.getH5Refer(),
					userInfoLog.getUID(),
					userInfoLog.getH5ParentAccId(),					
					appInfo[1] 
			};
			valueSuffix = "R";			
		}
		
		if (keyFields == null) {
			return;
		}
		
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		mapValueObj.setSuffix(valueSuffix);
		context.write(mapKeyObj, mapValueObj);
	
	}

}
