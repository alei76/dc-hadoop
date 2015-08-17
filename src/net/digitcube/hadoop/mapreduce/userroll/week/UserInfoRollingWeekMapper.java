package net.digitcube.hadoop.mapreduce.userroll.week;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoWeekRolling;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月31日 下午8:49:51 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserInfoRollingWeekMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	//加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);

		String fileSuffix = ((FileSplit) context.getInputSplit()).getPath()
				.getName();
		String[] keyFields = null;
		String[] valueFields = null;
		if (fileSuffix.contains(Constants.SUFFIX_USERROLLING_EVERY_WEEK)) { // 用户滚存日志
			//String appid = paraArr[0];
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(paraArr[0])){
				return;
			}
			
			String[] appInfo = paraArr[0].split("\\|");
			String platform = paraArr[1];
			String channel = paraArr[2];
			String gameRegion = paraArr[3];
			String accountID = paraArr[4];
			// boolean isFirstLogin = "1".equals(paraArr[5]);
			// boolean isFirstPay = "1".equals(paraArr[6]);
			// int loginTimes = StringUtil.convertInt(paraArr[7], 0);
			// int onlineTime = StringUtil.convertInt(paraArr[8], 0);
			// int onlineDay = StringUtil.convertInt(paraArr[9], 0);
			// int currencyAmount = StringUtil.convertInt(paraArr[10], 0);
			// int payTimes = StringUtil.convertInt(paraArr[11], 0);
			/*keyFields = new String[] { appid, platform, accountID };
			valueFields = new String[] { "A", channel, gameRegion, paraArr[5],
					paraArr[6], paraArr[7], paraArr[8], paraArr[9],
					paraArr[10], paraArr[11] };*/
			
			keyFields = new String[] { 
					appInfo[0], // see @UserInfoRollingDayMapper 
					platform, 
					accountID,
					gameRegion
			};
			valueFields = new String[] { "A", channel, gameRegion, paraArr[5],
					paraArr[6], paraArr[7], paraArr[8], paraArr[9],
					paraArr[10], paraArr[11],
					appInfo[1]
			};
		} else if (fileSuffix.contains(Constants.SUFFIX_USERROLLING_WEEK)) {
			//UserInfoWeekRolling userInfoWeekRolling = new UserInfoWeekRolling(paraArr);
			UserInfoWeekRolling userInfoWeekRolling = new UserInfoWeekRolling(scheduleTime, paraArr);
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(userInfoWeekRolling.getAppID())){
				return;
			}
			
			/*keyFields = new String[] { userInfoWeekRolling.getAppID(),
					userInfoWeekRolling.getPlatform(),
					userInfoWeekRolling.getAccountID() };
			valueFields = new String[] { "B",
					userInfoWeekRolling.getInfoBase64() };*/
			
			String[] appInfo = userInfoWeekRolling.getAppID().split("\\|");
			keyFields = new String[] { 
					appInfo[0],
					userInfoWeekRolling.getPlatform(),
					userInfoWeekRolling.getAccountID(),
					userInfoWeekRolling.getPlayerWeekInfo().getGameRegion()
			};
			valueFields = new String[] { 
					"B",
					userInfoWeekRolling.getInfoBase64(),
					appInfo[1]
			};
		}
		if (keyFields == null) {
			return;
		}
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valueFields);
		context.write(mapKeyObj, mapValueObj);
	}
}
