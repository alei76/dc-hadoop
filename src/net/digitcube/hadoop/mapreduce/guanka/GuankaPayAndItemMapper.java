package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 主要逻辑
 * 对玩家当天的每个关卡输出一条记录，统计信息包括：
 * 开始次数，成功次数，失败次数，失败退出次数，总时长，成功总时长，失败总时长
 * 
 * 输出：
 * appID, platform, channel, gameServer, accountId, levelId, isNew(Y/N), isPay(Y/N),
 * beginTimes, successTime, failedTimes, failedExitTimes, totalDuration, succDuration, failedDuration
 */

public class GuankaPayAndItemMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private IntWritable mapValObj = new IntWritable();
	String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		
		// A. 关卡内道具购买和消耗
		if(fileName.contains(Constants.DESelf_ItemBuy)
				|| fileName.contains(Constants.DESelf_ItemUse)){
			
			EventLog eventLog = new EventLog(array);
			String appID = eventLog.getAppID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(appID)){
				return;
			}
			
			Map<String, String> map = eventLog.getArrtMap();
			String guankaId = map.get("levelsId");
			String itemId = map.get("itemId");
			
			if(StringUtil.isEmpty(guankaId) || StringUtil.isEmpty(itemId)){
				return;
			}
			
			// 道具购买/消耗
			String type = Constants.DIMENSION_GUANKA_ITEM_BUY;
			if(fileName.contains(Constants.DESelf_ItemUse)){
				type = Constants.DIMENSION_GUANKA_ITEM_USE;
			}
			
			//真实区服
			String[] keyFields = new String[]{
					appID,
					platform,
					channel,
					gameServer,
					Constants.PLAYER_TYPE_ONLINE,
					guankaId, //vkey1
					type, //type
					itemId //vkey2
			};
						
			mapKeyObj.setSuffix(Constants.SUFFIX_GUANKA_ITEM);
			mapKeyObj.setOutFields(keyFields);
			mapValObj.set(1);
			context.write(mapKeyObj, mapValObj);
			
			//全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
			
		}else if(fileName.contains(Constants.LOG_FLAG_PAYMENT)) {//关卡内付费
			
			PaymentLog paymentLog = null;
			try{
				//日志里某些字段有换行字符，导致这条日志换行而不完整，
				//用 try-catch 过滤掉这类型的错误日志
				paymentLog = new PaymentLog(array);
			}catch(Exception e){
				//TODO do something to mark the error here
				return;
			}
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(paymentLog.getAppID())){
				return;
			}
			
			if(null == paymentLog.getGuankaId() || paymentLog.getCurrencyAmount() <= 0){
				return;
			}
			
			//关卡付费次数
			String[] keyFields = new String[]{
					paymentLog.getAppID(),
					paymentLog.getPlatform(),
					paymentLog.getChannel(),
					paymentLog.getGameServer(),
					Constants.PLAYER_TYPE_PAYMENT,
					paymentLog.getGuankaId(), // vkey1
					Constants.DIMENSION_GUANKA_PAYMENT, // type
					Constants.DIMENSION_GUANKA_PAYMENT_times // vkey2
			};
			
			mapKeyObj.setSuffix(Constants.SUFFIX_GUANKA_PAY);
			mapKeyObj.setOutFields(keyFields);
			mapValObj.set(1);
			context.write(mapKeyObj, mapValObj);
			
			//关卡付费次数-->全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
			
			//关卡付费金额
			keyFields = new String[]{
					paymentLog.getAppID(),
					paymentLog.getPlatform(),
					paymentLog.getChannel(),
					paymentLog.getGameServer(),
					Constants.PLAYER_TYPE_PAYMENT,
					paymentLog.getGuankaId(), // vkey1
					Constants.DIMENSION_GUANKA_PAYMENT, // type
					Constants.DIMENSION_GUANKA_PAYMENT_amount //vkey2
			};
			// 20141208 : 与 VV 约定：
			// 为了不遗失小数，入库前关卡内付费金额*1000，取出后需/1000
			float payment = 1000 * paymentLog.getCurrencyAmount();
			mapValObj.set(new Float(payment).intValue());
			
			mapKeyObj.setSuffix(Constants.SUFFIX_GUANKA_PAY);
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, mapValObj);
			
			//关卡付费金额-->全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			context.write(mapKeyObj, mapValObj);
		}
	}
}
