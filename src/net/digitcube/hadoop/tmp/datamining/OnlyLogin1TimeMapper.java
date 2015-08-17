package net.digitcube.hadoop.tmp.datamining;


import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 主要逻辑：
 * 统计在 20140201 ~ 20140910 时间点内一次游戏玩家（新增）占比
 * 即：在该时段内新增且只登录过一次游戏的玩家，占该时间段内新增玩家的比例
 */
public class OnlyLogin1TimeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable(1);
	private Date scheduleTime = new Date();
	private int newAddTime1 = 0;
	private int newAddTime2 = 0;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Calendar cal = Calendar.getInstance();
		cal.set(2014, 1, 1, 0, 0, 0);//20140201
		newAddTime1 = (int)(cal.getTimeInMillis()/1000);
		
		cal.set(2014, 8, 10, 0, 0, 0);//20140910
		newAddTime2 = (int)(cal.getTimeInMillis()/1000);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog user = new UserInfoRollingLog(scheduleTime, paraArr);
		if(!MRConstants.ALL_GAMESERVER.equals(user.getPlayerDayInfo().getGameRegion())){
			return;
		}
		
		int newAddDay = user.getPlayerDayInfo().getFirstLoginDate();
		//在 20140201 ~ 20140910 时间内新增
		if(newAddTime1 <= newAddDay && newAddDay <= newAddTime2){
			String appid = user.getAppID().split("\\|")[0];
			String platform = user.getPlatform();
			int totalLoginTimes = user.getPlayerDayInfo().getTotalLoginTimes();
			keyObj.setOutFields(new String[]{
					appid,
					platform
			});
			valObj.set(totalLoginTimes);
			context.write(keyObj, valObj);
		}
	}
}
