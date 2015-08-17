package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.UIDRollingLog;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计每天的日志量：大小和行数
 */
public class UIDIncrementMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text keyObj = new Text();
	private IntWritable one = new IntWritable(1);
	static Calendar cal = Calendar.getInstance();
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UIDRollingLog uidRollingLog = new UIDRollingLog(paraArr);
		
		if(!MRConstants.ALL_GAMESERVER.equals(uidRollingLog.getGameServer())){
			return;
		}
		
		int firstLoginDate = uidRollingLog.getFirstLoginDate();
		cal.setTimeInMillis(1000L*firstLoginDate);
		String month = sdf.format(cal.getTime());
		keyObj.set(month);
		context.write(keyObj, one);
	}
	
	public static void main(String[] args){
		System.out.println(cal.get(Calendar.MONTH));
		System.out.println(sdf.format(cal.getTime()));
	}
}
