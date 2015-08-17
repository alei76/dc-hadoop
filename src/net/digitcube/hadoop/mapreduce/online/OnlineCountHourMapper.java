package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author 
 * @version 1.0 
 *          输入：<br>
 *          APPID UID AccountID LoginTime Channel AccountType Gender Age GameServer Resolution 
 *          OperSystem Brand NetType Country Province Operators  OnlineTime Level<br>
 *          输出：<br>
 *          key = APPID,platform,gameregion<br>
 *          value = 1
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class OnlineCountHourMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);

	private final static int Index_Appid = 0;
	private final static int Index_Platform = 1;
	private final static int Index_Channel = 4;
	private final static int Index_gameServer = 8;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = new String[] { valueArr[Index_Appid],
				valueArr[Index_Platform], 
				//valueArr[Index_Channel],
				valueArr[Index_gameServer]};
		
		OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, one);
	}
}
