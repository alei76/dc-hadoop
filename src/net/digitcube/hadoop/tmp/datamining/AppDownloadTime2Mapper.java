package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AppDownloadTime2Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable duration = new IntWritable(0);
	private IntWritable times = new IntWritable(1);


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			eventLog = new EventLog(arr);
		}catch(Exception e){
			return;
		}
		
		if(eventLog.getEventId().contains(Constants.DESELF_APP_DOWNLOAD_COMPLETE) && 0 != eventLog.getDuration()){
			duration.set(eventLog.getDuration());
			context.write(duration, times);
		}
	}
}
