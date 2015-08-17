package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算当天活跃用户<br>
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午3:43:17 @copyrigt www.digitcube.net <br>
 * <br>
 *          输入，小时登陆处理结果：<br>
 *          appid,platform,accountid,logintime,
 *          channel,accounttype,gender,age,gameserver,resolution, opersystem
 *          ,brand,nettype,country,province,operators,MAXonlinetime,MAXlevel <br>
 *          输出：<br/>
 *          key:appid,platform,accountid<br>
 *          value:logintime,channel,accounttype,gender,age,gameserver,resolution
 *          , opersystem,brand,nettype,country,province,operators,
 *          TotalOnlinetime, MAXlevel
 * 
 */


/**
 * use @UserOnlineDayMapper and @UserOnlineDayReducer instead
 */
@Deprecated
public class UserLoginDayMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	public final static int Index_Appid = 0;
	public final static int Index_Platform = 1;
	public final static int Index_Accountid = 2;
	public final static int Index_Onlinetime = 3;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = new String[] { onlineArr[Index_Appid],
				onlineArr[Index_Platform], onlineArr[Index_Accountid] };

		mapKeyObj.setOutFields(keyFields);
		String[] valueFields = new String[15];
		System.arraycopy(onlineArr, Index_Onlinetime, valueFields, 0,
				valueFields.length);

		mapValueObj.setOutFields(valueFields);

		context.write(mapKeyObj, mapValueObj);
	}
}
