package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午5:05:26 @copyrigt www.digitcube.net<br/>
 *          输入：<br/>
 *          key : appid,platform,accountid<br/>
 *          value:channel,accounttype,gender,age,gameserver,resolution,
 *          opersystem ,brand,nettype,country,province,operators,CurrencyAmount<br>
 *          输出:<br/>
 *          net.digitcube.hadoop.mapreduce.domain.PaymentDayLog
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution ,opersystem,brand,nettype,country,province,operators
 *          ,CurrencyAmount(当日总付费),TotalPayTimes(当日付费次数)
 */

public class PaymentDayReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	public static int Index_CurrencyAmount = 12;
	public static int Index_Level = 13;
	public static int Index_PayTime = 14;
	public static int Index_AppVersion = 15;

	//保存玩家当天每次付费的信息
	//并按每次付费的时间进行排序
	private TreeMap<String, String> map = new TreeMap<String, String>();
	
	private StringBuffer sb = new StringBuffer();
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}


	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		map.clear();
		sb.delete(0, sb.length());
		
		if(Constants.SUFFIX_PAYMENT_DAY.equals(key.getSuffix())){
			// 对同一个玩家当天的付费金额相加去重统计
			//key.setSuffix(Constants.SUFFIX_PAYMENT_DAY);
		
			// 计算用户最高等级，最大在线时间
			float totalCurrencyAmount = 0;
			int totalPaytimes = 0;
			String[] defaultLastValue = null; // 随机取一个其他值
			PaymentDayLog paymentDayLog = new PaymentDayLog();
			String[] keyArray = key.getOutFields();
			paymentDayLog.setAppID(keyArray[PaymentDayLog.INDEX_AppID]);
			paymentDayLog.setPlatform(keyArray[PaymentDayLog.INDEX_Platform]);
			paymentDayLog.setAccountID(keyArray[PaymentDayLog.INDEX_AccountID]);

			String lastPayTime = "";
			String UID = ""; //iOS 渠道修正用
			String maxVersion = "";//20140723:对同一个玩家不同版本信息进行揉合，取最大版本号
			for (OutFieldsBaseModel val : values) {
				UID = val.getSuffix();
				String[] value = val.getOutFields();
	
				float currencyAmount = StringUtil.convertFloat(value[Index_CurrencyAmount], 0);
				totalCurrencyAmount += currencyAmount;
				totalPaytimes++;
				
				// 当天最多保存 100 个付费记录
				/*20140429：在玩家生命轨迹中需要记录玩家所有付费情况：付费时间、次数、金额
				 * 对于单机型游戏，可能会在本地累积很多付费记录（如好几天），某天联网时再把所有记录上报上来
				 * 所以这里把个数限制取消
				 * 
				if(map.size() <= 100){
					sb.delete(0, sb.length());
					String payTime = defaultLastValue[defaultLastValue.length - 1];
					String level = defaultLastValue[defaultLastValue.length - 2];
					// payTime:currencyAmount:level
					sb.append(payTime).append(":").append(currencyAmount).append(":").append(level);
					map.put(payTime, sb.toString());
				}*/

				String level = value[Index_Level];
				String payTime = value[Index_PayTime];
				// 保存所有 次付费记录：payTime:currencyAmount:level
				sb.append(payTime).append(":").append(currencyAmount).append(":").append(level).append(",");
				
				//其它信息取最后一次付费时信息
				if(lastPayTime.compareTo(payTime) < 0){
					defaultLastValue = value;
					UID = val.getSuffix();
				}
				
				//取最大版本号
				String currentVerion = value[Index_AppVersion];
				if(maxVersion.compareTo(currentVerion) < 0){
					maxVersion = currentVerion;
				}
			}
			//设置最大版本号
			paymentDayLog.setAppID(paymentDayLog.getAppID() + "|" + maxVersion);
			paymentDayLog.setExtend(new CommonExtend(defaultLastValue, -5));
			paymentDayLog.setCurrencyAmount(totalCurrencyAmount);// 总付费金额
			paymentDayLog.setTotalPayTimes(totalPaytimes);// 总付费次数
			
			//保存所有 次付费记录
			String payRecords = sb.length()>0 ? sb.substring(0, sb.length() -1) : "-";
			paymentDayLog.setPayRecords(payRecords);
			
			//Added at 20140606 : iOS 渠道修正
			//20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
			//if(MRConstants.PLATFORM_iOS_STR.equals(paymentDayLog.getPlatform())){
				String reviseChannel = IOSChannelUtil.checkForiOSChannel(paymentDayLog.getAppID(), 
																	UID, 
																	paymentDayLog.getPlatform(), 
																	paymentDayLog.getExtend().getChannel());
				paymentDayLog.getExtend().setChannel(reviseChannel);
			//}
			
			key.setOutFields(paymentDayLog.toStringArray());
			context.write(key, NullWritable.get());
			
		}else if(Constants.SUFFIX_PAYMENT_LAYOUT_ON_LEVEL.equals(key.getSuffix())){
			// 对同一个付费等级所有玩家的进行付费人次和付费金额的统计
			
			OutFieldsBaseModel valObj = new OutFieldsBaseModel();
			valObj.setSuffix(Constants.SUFFIX_PAYMENT_LAYOUT_ON_LEVEL);
			
			// 对同一个付费等级所有玩家的进行付费人次和付费金额的统计
			int totalPaytimes = 0;
			float totalCurrencyAmount = 0;
			for (OutFieldsBaseModel val : values) {
				String amount = val.getOutFields()[0];
				float currencyAmount = StringUtil.convertFloat(amount, 0);
				totalCurrencyAmount += currencyAmount;
				totalPaytimes++;
			}
			
			// 等级付费金额
			String[] keyFields = key.getOutFields();
			String[] payLeCurrency = new String[]{
					keyFields[0],	// appId
					keyFields[1],	// platform
					keyFields[2],	// channel
					keyFields[3],	// gameServer
					Constants.DIMENSION_PAY_LEVEL_CURRENCY,	// 维度：等级付费金额
					keyFields[4],	// 维度值：那个等级
					""+totalCurrencyAmount	// 该级别的付费总金额
			};
			valObj.setOutFields(payLeCurrency);
			context.write(valObj, NullWritable.get());
			
			// 等级付费人次
			String[] payLeTimes = new String[]{
					keyFields[0],	// appId
					keyFields[1],	// platform
					keyFields[2],	// channel
					keyFields[3],	// gameServer
					Constants.DIMENSION_PAY_LEVEL_TIMES,	// 维度：等级付费金额
					keyFields[4],	// 维度值：那个等级
					""+totalPaytimes	// 该级别的付费总人次（次数）
			};
			valObj.setOutFields(payLeTimes);
			context.write(valObj, NullWritable.get());
		}else{
			context.write(key, NullWritable.get());
		}
	}

}
