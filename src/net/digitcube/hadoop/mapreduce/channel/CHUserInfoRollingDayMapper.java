package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Date;
import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import net.digitcube.hadoop.model.channel.UserInfoLog2;
import net.digitcube.hadoop.model.channel.UserInfoRollingLog2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 用户滚存信息
 */
public class CHUserInfoRollingDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static String DATA_FLAG_ROLLING = "R";
	public static String DATA_FLAG_ONLINE = "O";
	public static String DATA_FLAG_INFO = "I";
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	// 加入 scheduleTime 是为了处理 JCE 编码由 GBK 调整为 UTF-8 的兼容
	private Date scheduleTime = null;

	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_ROLLING)) { // 用户滚存日志
			UserInfoRollingLog2 userInfoRollingLog = new UserInfoRollingLog2(scheduleTime, paramArr);
			
			String[] keyFields = new String[] {
					userInfoRollingLog.getAppId(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getUid()};
			
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(paramArr);
			mapValueObj.setSuffix(DATA_FLAG_ROLLING);
			context.write(mapKeyObj, mapValueObj);
			
		} else if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_USERINFO)) { // 用户激活日志
			UserInfoLog2 userInfoLog2 = new UserInfoLog2(paramArr);
			
			String[] keyFields = new String[] {
					userInfoLog2.getAppID(), 
					userInfoLog2.getPlatform(),
					userInfoLog2.getUID() 
			};
			
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(paramArr);
			mapValueObj.setSuffix(DATA_FLAG_INFO);
			context.write(mapKeyObj, mapValueObj);
			
		} else if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_ONLINE)) { // 活跃用户日志
			OnlineDayLog2 onlineDayLog2 = new OnlineDayLog2(paramArr);			
			
			String[] keyFields = new String[] {
					onlineDayLog2.getAppId(), 
					onlineDayLog2.getPlatform(),
					onlineDayLog2.getUid()
			};
			
			mapKeyObj.setOutFields(keyFields);
			mapValueObj.setOutFields(paramArr);
			mapValueObj.setSuffix(DATA_FLAG_ONLINE);
			context.write(mapKeyObj, mapValueObj);
		} 
	}
}
