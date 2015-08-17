package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.HadoopUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算当天活跃用户<br>
 * 
 * @author rickpan
 * @version 1.0 
 * <br>
 *          输入自然周或自然月每天登陆去重处理后的结果：<br>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution,opersystem,brand,nettype,country,province,operators,
 *          lastlogintime(最后登陆时间),totalOnlineTime (总在线时长), maxLevel(最高等级),
 *          totalLoginTimes(登陆总次数)
 *          输出：<br/>
 *          key:appid,platform,accountid<br>
 *          value:
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution,opersystem,brand,nettype,country,province,operators,
 *          lastlogintime(最后登陆时间),totalOnlineTime (总在线时长), maxLevel(最高等级),
 *          totalLoginTimes(登陆总次数)
 * 
 */

public class UserLoginWeekMonthMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	public final static int Index_Appid = 0;
	public final static int Index_Platform = 1;
	public final static int Index_Accountid = 2;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * 检查用户是否已指定日志的输出后缀
		 * Constants.KEY_ONLINE_NATURE_WEEK 和 Constants.KEY_ONLINE_NATURE_MONTH
		 * 必须有且只能有一个为 true
		 * 如果是自然周任务，则加上自然周后缀，如果是自然月任务，则加上自然月后缀
		 */
		HadoopUtil.ensureNatureWeekOrMonthIsSet(context.getConfiguration(),
												Constants.KEY_ONLINE_NATURE_WEEK,
												Constants.KEY_ONLINE_NATURE_MONTH);
		
		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = new String[] { onlineArr[Index_Appid],
											onlineArr[Index_Platform], 
											onlineArr[Index_Accountid] };

		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(onlineArr);

		context.write(mapKeyObj, mapValueObj);
	}
}
