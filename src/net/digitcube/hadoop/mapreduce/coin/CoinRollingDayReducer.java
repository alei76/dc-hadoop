package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.Constants.UserLostType;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CoinRollingDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	OutFieldsBaseModel redObj = new OutFieldsBaseModel();
	
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		Calendar calendar = Calendar.getInstance();
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statDate = (int)(calendar.getTimeInMillis() / 1000);
	}


	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		String[] valFields = null;
		String[] appVerArr = {null,null};
		
		String[] coinRollArray = null;
		String[] coinLogArray = null;
		
		int maxSeqno = 0;
		long finalCoinNum = 0;
		for (OutFieldsBaseModel value : values) {
			String[] array = value.getOutFields();
			if ("A".equals(array[0])) {
				coinRollArray = array;
				//设置 APP 版本号
				appVerArr[0] = coinRollArray[1];
			} else if ("B".equals(array[0])) {
				coinLogArray = array;
				long coinNum = StringUtil.convertLong(array[2], 0);
				int seqno = StringUtil.convertInt(array[3], 0);
				if (seqno > maxSeqno) {
					maxSeqno = seqno;
					finalCoinNum = coinNum;
				}
				
				//新注册的玩家滚存里还不存在，取注册激活里日志里的 APP 版本号
				appVerArr[1] = coinLogArray[1];
			}
		}
		
		//app 版本优先级 ：当天虚拟币日志的版本优先于滚存的版本
		String newestVersion = null == appVerArr[1] ? appVerArr[0] : appVerArr[1];
		//用最新的 app 版本重新设置回 appid
		key.getOutFields()[0] = key.getOutFields()[0] + "|" + newestVersion;
		
		//该玩家今天没在线上报数据
		if(null == coinLogArray){
			valFields = new String[]{
					coinRollArray[2], // coinNum
					coinRollArray[3]  // lastUpdateDate
			};
		}else{
			valFields = new String[]{
					finalCoinNum+"", //  maxCoinNum
					statDate+""  // lastUpdateDate
			};
		}
		
		key.setSuffix(Constants.SUFFIX_COIN_ROLLING);
		redObj.setOutFields(valFields);
		context.write(key, redObj);
	}
}
