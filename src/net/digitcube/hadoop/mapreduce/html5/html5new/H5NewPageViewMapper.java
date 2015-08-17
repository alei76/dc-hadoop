package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
  * 
 * 主要逻辑：
 * 对每玩家当天的 PV 进行去重汇总，输出每玩家的 PV 总数以及 PV 为 1 时的次数
 * 
 * 
 * 输入：EventLog(H5 自定义时间 DC_PV 的日志)
 * 
 * map：
 * key = appId, accountId
 * val = platform| H5_APP|H5_DOMAIN| H5_REF|H5_CRTIME |UID|loginTime|pvKey(page + "|" + optime)
 * 
 * reduce：
 * for val : values
 * do
 *     if isNewPlayer
 *         isNewPlayer = true
 *     end if
 * 
 *     oldCount = pvMap.get(pvKey)
 *     pvMap.put( pvKey , oldCount＋ 1)
 * done
 * 
 * playerTotalPVs = 0
 * player1ViewPvs = 0
 * playerPVRecords = ""
 * for entry : pvMap
 * do
 *     playerTotalPVs += entry.getValue
 *     if (1 == entry.getValue)
 *         player1ViewPvs++
 *     enf if
 * 
 *     playerPVRecords += entry.getKey + entry.getValue
 * done
 * 
 * 输出：
 * appId|accountId|
 * platform|H5_APP| H5_DOMAIN| H5_REF|H5_CRTIME|UID|playerTotalPVs|player1ViewPvs|pvRecords
 * 
 */

public class H5NewPageViewMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

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

		String[] eventArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(eventArr);
		}catch(Exception e){
			return;
		}
		
		if(!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())){
			return;
		}
		
		String H5_PromotionAPP = eventLog.getH5PromotionApp();
		String H5_DOMAIN = eventLog.getH5Domain();
		String H5_REF = eventLog.getH5Refer();
		String H5_CRTIME = eventLog.getH5CRtTime();
		
		if(eventLog.isReferDomainEmpty()){
			return;
		}	

		String page = eventLog.getArrtMap().get("page");
		String openTime = eventLog.getArrtMap().get("optime");
		String loginTime = eventLog.getArrtMap().get("lttime");
		
		if(StringUtil.isEmpty(page)){
			return;
		}
		
		String[] keyFields = new String[] { 
				eventLog.getAppID(),				
				eventLog.getAccountID()
		};
		
		//每次 PV 以 页面名称和打开时间作为标记
		String pvKey = page + "|" + openTime;
		String[] valFields = new String[]{
				eventLog.getPlatform(),
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				H5_CRTIME,
				eventLog.getUID(),
				loginTime,
				pvKey
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
	}
}

