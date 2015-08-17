package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *输入：H5 的付费日志
 *输出：appId, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId, totalCurrency, totalPaytimes
 */
public class H5PaymentDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog paymentLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			paymentLog = new PaymentLog(paymentArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		if(!FieldValidationUtil.validateAppIdLength(paymentLog.getAppID())){
			return;
		}
		
		//20140723: appId 中版本号，reduce 端处理时去取大版本号，其它信息揉合
		String[] appInfo = paymentLog.getAppID().split("\\|");
		String appId = appInfo[0];
		String appVersion = appInfo[1];
				
		//真实区服
		String[] keyFields = new String[] { 
				appId,
				paymentLog.getPlatform(),
				paymentLog.getAccountID(),
				paymentLog.getGameServer()
		};
		//全服
		String[] keyFields_AllGS = new String[] { 
				appId,
				paymentLog.getPlatform(),
				paymentLog.getAccountID(),
				MRConstants.ALL_GAMESERVER
		};
		
		//真实区服
		mapKeyObj.setOutFields(keyFields);
		//context.write(mapKeyObj, value);
		
		//全服
		mapKeyObj.setOutFields(keyFields_AllGS);
		context.write(mapKeyObj, value);
	}
}
