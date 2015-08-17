package net.digitcube.hadoop.mapreduce.channel.week;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoWeekRolling2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 注意：
 * 历史原因，在 g.dataeye.com 版本中，周的开始和结束为周一和周日
 * 
 * 原版：
 * 在 g.dataeye.com 版本的周滚存中
 * 玩家的 firstLoginWeekDate/firstPayWeekDate 是用一周的最后一天表示(即周日)
 * 因为 g.dataeye.com 版本数据库表中，周时间的表示字段为：weekId,beginDate,endDate
 *
 * 更改：
 * 在渠道版的周滚存中，
 * firstLoginWeekDate/firstPayWeekDate 改为用一周的开始一天表示(即周一)
 * 因为在渠道版数据库表中，周时间的表示字段为：weekBeginDate（本周的第一天）
 * 
 * 这样在周游戏行为或周留存时，直接把 weekBeginDate 输出入库即可
 */
public class CHUserInfoRollingWeekMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	static final String DATA_FLAG_WEEK_INFO = "W";
	static final String DATA_FLAG_ROLL_INFO = "R";
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();

	//加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;
	private String fileSuffix = null;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_WEEK_INFO)) {//天滚出输出玩家周游戏情况
			int i = 0;
			String appId = arr[i++];
			String appVer = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String country = arr[i++];
			String province = arr[i++];
			String uid = arr[i++];
			
			String[] keyFields = new String[]{
					appId,
					platform,
					uid
			};
			
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(arr);
			mapValObj.setSuffix(DATA_FLAG_WEEK_INFO);
			context.write(mapKeyObj, mapValObj);
			
		} else if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_ROLLING_WEEK)) {//周历史滚存
			
			UserInfoWeekRolling2 userInfoWeekRolling = new UserInfoWeekRolling2(scheduleTime, arr);
			String[] keyFields = new String[] { 
					userInfoWeekRolling.getAppId(),
					userInfoWeekRolling.getPlatform(),
					userInfoWeekRolling.getUid(),
			};
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(arr);
			mapValObj.setSuffix(DATA_FLAG_ROLL_INFO);
			context.write(mapKeyObj, mapValObj);
		}
	}
}
