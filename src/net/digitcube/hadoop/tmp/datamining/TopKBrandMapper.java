package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKBrandMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text mapKeyObj = new Text();
	IntWritable val = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineDayLog onlineDayLog = null;
		try{
			onlineDayLog = new OnlineDayLog(onlineArr);
		}catch(Throwable t){
			return;
		}

		if(!MRConstants.ALL_GAMESERVER.equals(onlineDayLog.getExtend().getGameServer())){
			return;
		}
		mapKeyObj.set(onlineDayLog.getExtend().getBrand());
		context.write(mapKeyObj, val);
	}
}
