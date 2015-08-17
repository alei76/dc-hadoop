package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5UserInfoDayLog;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
  * 
 * 主要逻辑：
 * 把 玩家注册、OnlineDayForPlayer、PageViewForPlayer 的信息揉合到一起
 * 下游 MR 根据这些信息统计有病毒传播产生的 UV/PV 等
 * 
 * 输出：
 * appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId, 
 * parentAccountId, totalLoginTimes, totalOnlineTime, playerTotalPVs, uniqIpCount
 */

public class H5VirusSpreadForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String FLAG_ONLINE = "OL";
	public static final String FLAG_PV = "PV";
	public static final String FLAG_USERINFO = "INFO";
	
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
			String totalLoginTimes = h5OnlineDayLog.getTotalLoginTimes() + "";
			String totalOnlineTime = h5OnlineDayLog.getTotalOnlineTime() + "";
			String ipRecords = h5OnlineDayLog.getIpRecords();
			
			String[] keyFields = new String[] { 
					appId,
					platform,
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					accountId
			};
			
			String[] valFields = new String[]{
					totalLoginTimes,
					totalOnlineTime,
					ipRecords
			};
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(FLAG_ONLINE);
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(Constants.SUFFIX_H5_PV_FOR_PLAYER)){
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(arr[0])){
				return;
			}
			if(arr.length<10){
				return;
			}
			String appId = arr[i++];
			String platform = arr[i++];
			String H5_PromotionAPP = arr[i++];
			String H5_DOMAIN = arr[i++];
			String H5_REF = arr[i++];
			String accountId = arr[i++];
			String isNewPlayer = arr[i++];
			String playerTotalPVs = arr[i++];
			String player1ViewPvs = arr[i++];
			String playerPVRecords = arr[i++];
			
			String[] keyFields = new String[] { 
					appId,
					platform,
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					accountId
			};
			
			String[] valFields = new String[]{
					playerTotalPVs
			};
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(FLAG_PV);
			context.write(keyObj, valObj);
			
		}else if(fileName.contains(Constants.SUFFIX_H5_NEW_USERINFO_DAY)){
			H5UserInfoDayLog h5UserInfoDayLog = null;	
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				h5UserInfoDayLog = new H5UserInfoDayLog(arr);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}		
			
			String H5_PromotionAPP = h5UserInfoDayLog.getH5app();
			String H5_DOMAIN = h5UserInfoDayLog.getH5domain();
			String H5_REF = h5UserInfoDayLog.getH5ref();
			String H5_PARENT_ACCID = h5UserInfoDayLog.getParentAccoutId();		
			
			String[] keyFields = new String[] { 
					h5UserInfoDayLog.getAppId(),
					h5UserInfoDayLog.getPlatform(),
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					h5UserInfoDayLog.getAccountId()
			};
			
			String[] valFields = new String[]{
					H5_PARENT_ACCID
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(FLAG_USERINFO);
			context.write(keyObj, valObj);
		}
	}
}
