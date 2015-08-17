package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class OnlineDayCheckMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable val1 = new IntWritable(1);
	private IntWritable val2 = new IntWritable();
	
	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		UserInfoRollingLog player = new UserInfoRollingLog(scheduleTime, paraArr);
		
		if(!player.getAppID().contains("72DB49D5674018190AEE079EB7920419")
				|| !MRConstants.ALL_GAMESERVER.equals(player.getPlayerDayInfo().getGameRegion())){
			return;
		}
			
		for(OnlineDay day : player.getPlayerDayInfo().getOnlineDayList()){
			val2.set(day.getOnlineDate());
			context.write(val1, val2);
		}
	}

}
