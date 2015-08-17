package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
  * 
 * 主要逻辑：
 * 对每玩家当天在线流水进行去重汇总，汇总信息包括：
 * 总登录次数、总在线时长、在线记录、独立 ip 数
 * 
 * 
 * 输入：OnlineLog(H5 每小时在线日志)
 * 
 * map：
 * key = appId| accountId
 * val = platform|H5_APP| H5_DOMAIN| H5_REF|H5_CRTIME|UID|ip|loginTime|onlineTime
 * 
 * ---reduce---：
 * for online : onlineList
 * do
 *     if online.isNewPlayer
 *         isNewPlayer = true
 *     end if
 * 
 *     playerIpSet.add( ip ) 
 * 
 *     onlineMap.put( loginTime_x, max(onlineTime) )
 * done
 * 
 * 输出：
 * appId| accountId| 
 * platform|H5_PromotionAPP| H5_DOMAIN| H5_REF|H5_CRTIME|UID|totalLoginTimes|totalOnlineTime|
    uniqIpCount|onlineRecords|ipRecords
 * 
 */

public class H5NewOnlineDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog onlineLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			onlineLog = new OnlineLog(onlineArr);
		}catch(Exception e){
			return;
		}
		
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}
		
		String[] appInfo = onlineLog.getAppID().split("\\|");
		if(appInfo.length < 2){
			return;
		}
		
		//20141223 : 与高境约定修改
		//a)时长大于 1 天则取 1 秒
		//b)时长小于等于 0 则取 1
		int onlineTime = onlineLog.getOnlineTime(); 
		onlineTime = onlineTime >= 24*3600 ? 1 : onlineTime;
		onlineTime = onlineTime <= 0 ? 1 : onlineTime;
		onlineLog.setOnlineTime(onlineTime);
		
		String appId = appInfo[0];
		String appVersion = appInfo[1];		
		String H5_PromotionAPP = onlineLog.getH5PromotionApp();
		String H5_DOMAIN = onlineLog.getH5Domain();
		String H5_REF = onlineLog.getH5Refer();
		String H5_CRTIME = onlineLog.getH5CRtTime();
		
		String accountType = onlineLog.getAccountType();
		String gender = onlineLog.getGender();
		String age = onlineLog.getAge();		
		String resolution = onlineLog.getResolution();
		String opSystem = onlineLog.getOperSystem();
		String brand = onlineLog.getBrand();
		String netType = onlineLog.getNetType();
		String country = onlineLog.getCountry();
		String province = onlineLog.getProvince();
		String operators = onlineLog.getOperators();
		String level = onlineLog.getLevel() + "";
		
		if(onlineLog.isReferDomainEmpty() || StringUtil.isEmpty(H5_CRTIME)){
			return;
		}
		
		String[] keyFields = new String[]{ 
				appId,		
				onlineLog.getAccountID()
		};

		String ip = onlineLog.getExtend_4();
		String[] valFields = new String[]{
				onlineLog.getPlatform(),
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				H5_CRTIME,
				onlineLog.getUID(),
				ip,
				onlineLog.getLoginTime() + "",
				onlineLog.getOnlineTime() + "",
				accountType, gender, age,resolution, opSystem,
				brand,netType,country,province,operators,level,
				appVersion
		};
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
	}
}
