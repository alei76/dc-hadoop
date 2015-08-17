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
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月16日 下午2:31:17 @copyrigt www.digitcube.net<br>
 *          输入：<br>
 *          appid1 IOS accountId0 1373891198 channelId 2 1 12 gameServer
 *          resolution operSystem brand 1 中国 广东省 u 718 0 <br>
 *          输出：<br>
 *          key = APPID,platform,gameregion,point <br>
 *          value = 1
 */

/**
 * use @AcuPcuCntHourMapper and @AcuPcuCntHourReducer instead
 */
@Deprecated
public class PCUHourMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	private final static int Index_Appid = 0;
	private final static int Index_Platform = 1;
	private final static int Index_LoginTime = 3;
	private final static int Index_OnlineTime = 16;
	
	private final static int Index_Channel = 4;
	private final static int Index_gameServer = 8;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] valueArr = value.toString().split(MRConstants.SEPERATOR_IN);

		int loginTime = StringUtil.convertInt(valueArr[Index_LoginTime], 0);
		int onlineTime = StringUtil.convertInt(valueArr[Index_OnlineTime], 0);

		Calendar calendar = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			calendar.setTime(date);
		}
		calendar.add(Calendar.HOUR_OF_DAY, -1); // 默认取调度初始化时间的前一个小时
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		int startPoint = (int) (calendar.getTimeInMillis() / 1000); // 找出起始时间点
		int maxPoint = startPoint + 3600; // 最大时间点

		if (loginTime > maxPoint || (loginTime + onlineTime) < startPoint) { // 登陆时间大于最大时间,或小于最小
			// ，直接跳过
			return;
		}

		if (loginTime > 0 && onlineTime > 0) {
			for (int point = (loginTime < startPoint ? startPoint : loginTime); point < maxPoint; point++) {
				if (loginTime <= point && onlineTime >= (point - loginTime)) {
					// 登陆时间在point点之前且在线时长大于point点-登陆时间记为一次在线
					
					/*
					 * ACU/PCU 计算调整为不分渠道统计，只按区服统计
					 * 在分区服统计的同时，另加一个不分区服的全量统计
					String[] keyFields = new String[] {valueArr[Index_Appid],
													   valueArr[Index_Platform],
													   valueArr[Index_Channel],
													   valueArr[Index_gameServer],
													   point + "" };
					mapKeyObj.setOutFields(keyFields);
					context.write(mapKeyObj, one);*/
					
					String[] keyFields = new String[] {valueArr[Index_Appid],
													   valueArr[Index_Platform],
													   valueArr[Index_gameServer],
													   point + "" };
					mapKeyObj.setOutFields(keyFields);
					context.write(mapKeyObj, one);
					
					
					// 不分区服的统计，gameServer 以 '-' 代替
					String[] keyFieldsAll = new String[] {valueArr[Index_Appid],
														   valueArr[Index_Platform],
														   MRConstants.INVALID_PLACE_HOLDER_CHAR,
														   point + "" };
					mapKeyObj.setOutFields(keyFieldsAll);
					context.write(mapKeyObj, one);
					
				}
				
				// 可以跳过了
				if (loginTime <= point && onlineTime <= (point - loginTime)) {
					break;
				}
			}
		}
	}
}
