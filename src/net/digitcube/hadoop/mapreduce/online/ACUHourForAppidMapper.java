package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午2:21:15 @copyrigt www.digitcube.net <br>
 * <br>
 *          输入：APPID,platform,gameregion,point,onlinenum在线人数<br>
 *          输出: key = APPID,platform,gameregion value = onlinenum在线人数<br>
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class ACUHourForAppidMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel outKey = new OutFieldsBaseModel();
	private IntWritable outValue = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		/*
		 * valueArr[3] = 渠道
		 * ACU 计算已经不再按渠道统计
		outKey.setOutFields(new String[] {valueArr[0], 
										  valueArr[1],
										  valueArr[2],
										  valueArr[3]});
		int playCount = StringUtil.convertInt(valueArr[5], 0);*/
		
		outKey.setOutFields(new String[] {valueArr[0], valueArr[1], valueArr[2]});
		int playCount = StringUtil.convertInt(valueArr[4], 0);

		outValue.set(playCount);
		context.write(outKey, outValue);
	}
}
