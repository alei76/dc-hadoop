package net.digitcube.hadoop.tmp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class LostPlayerLayoutMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	// 统计的数据时间
	int statTime = 0;
	int targetTime = 0;
	String yyyyMMdd = "";
	Date date = new Date();
	IntWritable one = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		//在输入路径中，从根目录开始，年份这个目录排在第几层
		//如 /data/digitcube/online_hour/2013/10/27/23/output 中年份排在第四
		//所以 year.index.in.path 的值应该设置为 4
		int yearIndex = 4;
		String input = ((FileSplit)context.getInputSplit()).getPath().toString();
		// 输入可能包含 'hdfs://'，将其去掉下面通过 '/' 分割是 year.index 才能正确
		input = input.replace("://",":"); 
		String[] components = input.split("/");
		
		//inputDirDate = yyyyMMdd
		/*yyyyMMdd = components[yearIndex]	//year
						 + components[yearIndex+1] //month
						 + components[yearIndex+2];*/
		
		int year = Integer.valueOf(components[yearIndex]);	//year
		int month = Integer.valueOf(components[yearIndex+1]); //month
		int day = Integer.valueOf(components[yearIndex+2]);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month-1);//calendar 中月份要减 1
		cal.set(Calendar.DATE, day-1);//结算数据时间减 1
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		statTime = (int)(cal.getTimeInMillis()/1000);
		if(statTime < 0){
			throw new RuntimeException("statTime is 0");
		}
		targetTime = statTime - 3 * 24 * 3600;
		cal.setTimeInMillis(1000L*targetTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
		yyyyMMdd = sdf.format(cal.getTime());
		
		context.getCounter("dc.statTime", ""+statTime).increment(1);
		context.getCounter("dc.targetTime", ""+targetTime).increment(1);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(date, paraArr);
		
		if(!userInfoRollingLog.getAppID().contains("27D0505D961F1903376D9E5DC18190AB")
				|| !userInfoRollingLog.getPlayerDayInfo().getGameRegion().equals(MRConstants.ALL_GAMESERVER)
				|| !MRConstants.PLATFORM_iOS_STR.equals(userInfoRollingLog.getPlatform())){
			return;
		}

		// 3 日流失，最后登录时间为 3 天前
		if(targetTime != userInfoRollingLog.getPlayerDayInfo().getLastLoginDate()){
			return;
		}
		
		System.out.println("3_DAY_LOST_ACCID_"+userInfoRollingLog.getAccountID());
		int level = userInfoRollingLog.getPlayerDayInfo().getLevel();
		int totalOnlineTime = userInfoRollingLog.getPlayerDayInfo().getTotalOnlineTime();
		/*int totalLoginTimes = userInfoRollingLog.getPlayerDayInfo().getTotalLoginTimes();
		int totalPayment = userInfoRollingLog.getPlayerDayInfo().getTotalCurrencyAmount();
		int totalPayTimes = userInfoRollingLog.getPlayerDayInfo().getTotalPayTimes();*/
		
		int onlineTimeItval = EnumConstants.getItval4OnlineTime(totalOnlineTime);
		/*int loginTimesItval = EnumConstants.getItval4DayLoginTimes(totalLoginTimes);
		int paymentRange = EnumConstants.getDayPayAmountRange2(totalPayment);
		if(0 == totalPayment){
			paymentRange = 0;
		}
		int payTimesRange = EnumConstants.getDayPayTimesRange(totalPayTimes);
		if(0 == totalPayTimes){
			payTimesRange = 0;
		}*/
		
		String suffix = null;
		String[] keyFields = null;
		suffix = yyyyMMdd + "_oltime";
		keyFields = new String[]{
				onlineTimeItval+"",
		};
		mapKeyObj.setOutFields(keyFields);
		mapKeyObj.setSuffix(suffix);
		context.write(mapKeyObj, one);
		
		suffix = yyyyMMdd + "_level";
		keyFields = new String[]{
				level+"",
		};
		mapKeyObj.setOutFields(keyFields);
		mapKeyObj.setSuffix(suffix);
		context.write(mapKeyObj, one);
		/*suffix = "loginTimesRange" + "_" + level;
		keyFields = new String[]{
				loginTimesItval+"",
		};
		mapKeyObj.setOutFields(keyFields);
		mapKeyObj.setSuffix(suffix);
		context.write(mapKeyObj, NullWritable.get());
		
		suffix = "paymentRange" + "_" + level;
		keyFields = new String[]{
				paymentRange+"",
		};
		mapKeyObj.setOutFields(keyFields);
		mapKeyObj.setSuffix(suffix);
		context.write(mapKeyObj, NullWritable.get());
		
		suffix = "payTimesRange" + "_" + level;
		keyFields = new String[]{
				payTimesRange+"",
		};
		mapKeyObj.setOutFields(keyFields);
		mapKeyObj.setSuffix(suffix);
		context.write(mapKeyObj, NullWritable.get());*/
	}
	
	public static void main(String[] args){
		int yearIndex = 4;
		String input = "hdfs://dcnamenode1:9090/data/digitcube/online_hour/2013/10/27/23/output";
		// 输入可能包含 'hdfs://'，将其去掉下面通过 '/' 分割是 year.index 才能正确
		input = input.replace("://",":"); 
		String[] components = input.split("/");
		
		//inputDirDate = yyyyMMdd
		int year = Integer.valueOf(components[yearIndex]);	//year
		int month = Integer.valueOf(components[yearIndex+1]); //month
		int day = Integer.valueOf(components[yearIndex+2]);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month-1);
		cal.set(Calendar.DATE, day-1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH:mm:ss"); 
		System.out.println(sdf.format(cal.getTime()));
	}
}
