package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AppDownloadTimeMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private String fileSuffix;
	private IntWritable duration = new IntWritable(0);
	private IntWritable times = new IntWritable(1);

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(fileSuffix.endsWith(Constants.DESELF_APP_DOWNLOAD_COMPLETE)){
			String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
			EventLog eventLog = new EventLog(arr);
			
			duration.set(eventLog.getDuration());
			
			context.write(duration, times);
		}
	}
}
