package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TmpRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();


	// 统计的数据时间
	private int statTime = 0;
	private int targetDate = 0;

	SimpleDateFormat sdf = null;
	Calendar calendar = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		sdf = new SimpleDateFormat("yyyyMMdd");
		calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_MONTH, 8);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);
		
		//14 日流失
		targetDate = statTime - 3600 * 24 * 14;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);


		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(new Date(), paraArr);
		
		if(!userInfoRollingLog.getAppID().contains("F9297CCFD28CA3FC087441AC87E6DC5E")){
			return;
		}

		int newAddDate = userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		boolean isNewUser = targetDate == newAddDate;
		boolean isEverLogin = userInfoRollingLog.isEverLogin(14);
		
		if (isNewUser && !isEverLogin) {
			calendar.setTimeInMillis(1000L * newAddDate);
			String newAddDateStr = sdf.format(calendar.getTime());
			String trace = Integer.toBinaryString(userInfoRollingLog.getPlayerDayInfo().track);
			String[] keyFields = new String[]{
					userInfoRollingLog.getAppID(),
					userInfoRollingLog.getAccountID(),
					userInfoRollingLog.getPlayerDayInfo().getLevel() + "",
					newAddDateStr,
					trace
			};
		
			mapKeyObj.setOutFields(keyFields);
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
