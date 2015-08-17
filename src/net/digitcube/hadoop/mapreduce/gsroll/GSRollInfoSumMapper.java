package net.digitcube.hadoop.mapreduce.gsroll;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 以滚存作为输入，以 appId+platform+accountId 为 key，玩家区服信息为 value
 * 在 reduce 端对统计玩家的所有滚服信息，并输出下列两项滚服相关信息：
 * a)滚服的新增活跃付费等，具体值如下
 * [appId, platform, channel, gameServer, fromGS, accountId, 
 *  isNewAdd, isPay, isNewAddDayPay, newAddDayPayAmount, isTodayFirstPay, firstPayAmount, isPayToday, todayPayAmount]
 *  
 * b)滚服留存情况
 * [appId, platform, channel, gameServer, fromGS, accountId, dayOffSet, playerType]
 * 
 * 输出：
 * a)人数统计
 * 		appId, platform, channel, gameServer, fromGS, playerType, num
 * b)付费统计
 * 		appId, platform, channel, gameServer, fromGS, dimenType, num/amount
 * 		其中 dimenType 为：pay-->当天付费,
 */
public class GSRollInfoSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, FloatWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private FloatWritable valObj = new FloatWritable(1);
	
	private String fileName = null;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		if(fileName.endsWith(Constants.SUFFIX_GS_ROLL_PLAYER_INFO)){
			
			statRollGSInfo(arr, context);
		}else if(fileName.endsWith(Constants.SUFFIX_GS_ROLL_30DAY_RETAIN)){
			
			statRollGS30DayRetain(arr, context);
		}
	}
	
	private void statRollGSInfo(String[] arr, Context context) throws IOException, InterruptedException{
		
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String fromGS = arr[i++];
		String accountId = arr[i++];
		String isNewAdd = arr[i++];
		String isPay = arr[i++];
		String isNewAddDayPay = arr[i++];
		String newAddDayPayAmount = arr[i++];
		String isTodayFirstPay = arr[i++];
		String firstPayAmount = arr[i++];
		String isPayToday = arr[i++];
		String todayPayAmount = arr[i++];
		
		//统计滚服人数-------------------------------
		keyObj.setSuffix(Constants.SUFFIX_GS_ROLL_PLAYER_NUM);
		//活跃玩家数
		String[] keyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer,
				fromGS,
				Constants.PLAYER_TYPE_ONLINE
		};
		keyObj.setOutFields(keyFields);
		valObj.set(1);
		context.write(keyObj, valObj);
		
		//新增玩家数
		if(Constants.DATA_FLAG_YES.equals(isNewAdd)){
			keyFields[keyFields.length-1] = Constants.PLAYER_TYPE_NEWADD;
			context.write(keyObj, valObj);
		}
		
		//付费玩家数
		if(Constants.DATA_FLAG_YES.equals(isPay)){
			keyFields[keyFields.length-1] = Constants.PLAYER_TYPE_PAYMENT;
			context.write(keyObj, valObj);
		}
		
		//统计滚服付费情况-------------------------------
		keyObj.setSuffix(Constants.SUFFIX_GS_ROLL_PAYMENT);
		//活跃玩家数
		String[] payKeyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer,
				fromGS,
				"NA"
		};
		
		//当天是否付费
		if(Constants.DATA_FLAG_YES.equals(isPayToday)){
			//付费金额
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_TODAY_PAY_CUR;
			valObj.set(StringUtil.convertFloat(todayPayAmount, 0));
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
			
			//付费人数
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_TODAY_PAY_NUM;
			valObj.set(1);
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
		}
		
		//新增当天是否付费
		if(Constants.DATA_FLAG_YES.equals(isNewAddDayPay)){
			//付费金额
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_FIRSTDAY_PAY_CUR;
			valObj.set(StringUtil.convertFloat(newAddDayPayAmount, 0));
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
			
			//付费人数
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_FIRSTDAY_PAY_NUM;
			valObj.set(1);
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
		}
		
		//首次付费
		if(Constants.DATA_FLAG_YES.equals(isTodayFirstPay)){
			//付费金额
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_FIRSTTIME_PAY_CUR;
			valObj.set(StringUtil.convertFloat(firstPayAmount, 0));
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
			
			//付费人数
			payKeyFields[payKeyFields.length-1] = Constants.DIMENSION_FIRSTTIME_PAY_NUM;
			valObj.set(1);
			keyObj.setOutFields(payKeyFields);
			context.write(keyObj, valObj);
		}
	}
	
	private void statRollGS30DayRetain(String[] arr, Context context) throws IOException, InterruptedException{
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String fromGS = arr[i++];
		String accountId = arr[i++];
		String dayOffSet = arr[i++];
		String playerType = arr[i++];
		
		String[] keyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer,
				fromGS,
				playerType,
				dayOffSet
		};
		
		keyObj.setOutFields(keyFields);
		valObj.set(1);
		keyObj.setSuffix(Constants.SUFFIX_GS_ROLL_30DAY_RETAIN_SUM);
		context.write(keyObj, valObj);
	}
}
