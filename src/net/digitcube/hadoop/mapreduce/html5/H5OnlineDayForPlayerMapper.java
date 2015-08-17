package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.FieldValidationUtil;

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
 * key = appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId
 * val = isNewPlayer, ip, loginTime, onlineTime
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
 * appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId, 
 * isNewPlayer, totalLoginTimes, totalOnlineTime, uniqIpCount, onlineRecords, playerIpSet
 * 
 */

public class H5OnlineDayForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

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
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(onlineLog.getAppID())){
			return;
		}
		
		String H5_PromotionAPP = onlineLog.getH5PromotionApp();
		String H5_DOMAIN = onlineLog.getH5Domain();
		String H5_REF = onlineLog.getH5Refer();
		
		if(onlineLog.isReferDomainEmpty()){
			return;
		}
		
		String[] keyFields = new String[]{ 
				onlineLog.getAppID(),
				onlineLog.getPlatform(),
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				onlineLog.getAccountID()
		};

		String isNewPlayer = onlineLog.isH5NewAddPlayer(statDate) 
			   	? Constants.DATA_FLAG_YES 
				: Constants.DATA_FLAG_NO;
		String ip = onlineLog.getExtend_4();
		String[] valFields = new String[]{
				isNewPlayer,
				ip,
				onlineLog.getLoginTime() + "",
				onlineLog.getOnlineTime() + "",
				onlineLog.getUID() // Added at 20141017
		};
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
	}
}
