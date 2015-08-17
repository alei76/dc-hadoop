package net.digitcube.hadoop.tmp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TmpLevelLayoutMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	// 统计的数据时间
	Date statTime = new Date();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(statTime, paraArr);
		
		if(!userInfoRollingLog.getAppID().contains("EA7D0701220EF0E4ACA26BC41B7C2AF9")
				|| !userInfoRollingLog.getPlayerDayInfo().getGameRegion().equals(MRConstants.ALL_GAMESERVER)){
			return;
		}

		int level = userInfoRollingLog.getPlayerDayInfo().getLevel();
		if(10 == level || 12 == level){
			int totalOnlineTime = userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime();
			int totalLoginTimes = userInfoRollingLog.getPlayerDayInfo().getTotalLoginTimes();
			int totalPayment = userInfoRollingLog.getPlayerDayInfo().getTotalCurrencyAmount();
			int totalPayTimes = userInfoRollingLog.getPlayerDayInfo().getTotalPayTimes();
			
			//int onlineTimeItval = EnumConstants.getItval4OnlineTime(totalOnlineTime);
			//int loginTimesItval = EnumConstants.getItval4DayLoginTimes(totalLoginTimes);
			int paymentRange = EnumConstants.getDayPayAmountRange2(totalPayment);
			if(0 == totalPayment){
				paymentRange = 0;
			}
			int payTimesRange = EnumConstants.getDayPayTimesRange(totalPayTimes);
			if(0 == totalPayTimes){
				payTimesRange = 0;
			}
			
			String suffix = null;
			String[] keyFields = null;
			/*suffix = "onlineTimeRange" + "_" + level;
			keyFields = new String[]{
					onlineTimeItval+"",
			};
			mapKeyObj.setOutFields(keyFields);
			mapKeyObj.setSuffix(suffix);
			context.write(mapKeyObj, NullWritable.get());
			
			suffix = "loginTimesRange" + "_" + level;
			keyFields = new String[]{
					loginTimesItval+"",
			};
			mapKeyObj.setOutFields(keyFields);
			mapKeyObj.setSuffix(suffix);
			context.write(mapKeyObj, NullWritable.get());*/
			
			/*suffix = "paymentRange" + "_" + level;
			keyFields = new String[]{
					paymentRange+"",
			};
			mapKeyObj.setOutFields(keyFields);
			mapKeyObj.setSuffix(suffix);
			context.write(mapKeyObj, NullWritable.get());*/
			
			suffix = "payTimesRange" + "_" + level;
			keyFields = new String[]{
					payTimesRange+"",
			};
			mapKeyObj.setOutFields(keyFields);
			mapKeyObj.setSuffix(suffix);
			context.write(mapKeyObj, NullWritable.get());
		}
	}
	
	public static void main(String[] args){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_MONTH, 8);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.add(Calendar.DAY_OF_MONTH, -14);
		System.out.println(sdf.format(calendar.getTime()));
	}
}
