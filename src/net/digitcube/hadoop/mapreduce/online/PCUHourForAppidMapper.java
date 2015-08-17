package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午2:21:15 @copyrigt www.digitcube.net<br>
 * <br>
 *          输入：APPID,platform,gameregion,point onlinenum在线人数<br>
 *          输出: key = APPID,platform,gameregion, value = point,onlinenum在线人数<br>
 */

/**
 * use @AcuPcuCntHourSumMapper and @AcuPcuCntHourSumReducer instead
 */
@Deprecated
public class PCUHourForAppidMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OutFieldsBaseModel outKey = new OutFieldsBaseModel();
		/*outKey.setOutFields(new String[] {valueArr[0], 
										  valueArr[1],
										  valueArr[2],
										  valueArr[3]});*/
		
		outKey.setOutFields(new String[] {valueArr[0], 
										  valueArr[1],
										  valueArr[2]});
		
		OutFieldsBaseModel outValue = new OutFieldsBaseModel();
		//outValue.setOutFields(new String[] { valueArr[4], valueArr[5] });
		outValue.setOutFields(new String[] { valueArr[3], valueArr[4] });
		
		context.write(outKey, outValue);
	}
}
