package net.digitcube.hadoop.mapreduce.habits;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 当日/前7/前30天活跃玩家数，首日/周/月付费数 <br>
 * 对应DAU WAU MAU FirstDayPayNum FirstWeekPayNum FirstMonthPayNum
 * 
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年7月30日 上午11:17:18 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserPay1730Mapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		//TCL 渠道字段尾部多写了一个字段，需做 trim 兼容
		for(int i=0; i< paraArr.length; i++){
			paraArr[i] = paraArr[i].trim(); 
		}
		mapKeyObj.setOutFields(paraArr);
		context.write(mapKeyObj, one);
	}
}
