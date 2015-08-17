package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoRollingLog2;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.reflect.TypeToken;

public class CHGameCenterMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	public static final String DATA_FLAG_ALL_USER = "AU";
	public static final String DATA_FLAG_NEW_USER = "NU";
	public static final String DATA_FLAG_DLD_USERS = "DU";
	public static final String DATA_FLAG_DLD_TIMES = "DT";
	
	private String[] one = new String[]{"1"};
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private TypeToken<Map<String, Integer>> type = new TypeToken<Map<String, Integer>>(){};
	
	private int statDate = 0;
	private Date scheduleTime = null;
	// 当前输入的文件后缀
	private String fileSuffix = "";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		statDate = DateUtil.getStatDate(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.contains(Constants.SUFFIX_CHANNEL_ROLLING)) {
			UserInfoRollingLog2 userInfoRollingLog = new UserInfoRollingLog2(scheduleTime, paramArr);
			String[] keyFields = new String[]{
					userInfoRollingLog.getAppId(),
					userInfoRollingLog.getPlatform(),
					userInfoRollingLog.getPlayerDayInfo().getChannel()
			};
			keyObj.setOutFields(keyFields);
			
			//数据中心-->累计安装数
			valObj.setOutFields(one);
			valObj.setSuffix(DATA_FLAG_ALL_USER);
			keyObj.setSuffix(Constants.SUFFIX_CHANNEL_GAME_CENTER);
			context.write(keyObj, valObj);
			
			//数据中心-->新增安装数
			if(statDate == userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate()){
				valObj.setSuffix(DATA_FLAG_NEW_USER);
				context.write(keyObj, valObj);
			}
			
		}else if(fileSuffix.contains(Constants.SUFFIX_CHANNEL_D_FOR_PLAYER)){
			int i = 0;
			String appId = paramArr[i++];
			String appVer = paramArr[i++];
			String platform = paramArr[i++];
			String channel = paramArr[i++];
			String country = paramArr[i++];
			String province = paramArr[i++];
			String uid = paramArr[i++];
			String isNewPlayer = paramArr[i++];
			String downloadTimes = paramArr[i++];
			String downloadRecords = paramArr[i++];
			
			String[] keyFields = new String[]{
					appId,
					platform,
					channel
			};
			keyObj.setOutFields(keyFields);
			
			//数据中心-->下载人数
			valObj.setOutFields(one);
			valObj.setSuffix(DATA_FLAG_DLD_USERS);
			keyObj.setSuffix(Constants.SUFFIX_CHANNEL_GAME_CENTER);
			context.write(keyObj, valObj);
			//数据中心-->下载次数
			valObj.setOutFields(new String[]{downloadTimes});
			valObj.setSuffix(DATA_FLAG_DLD_TIMES);
			context.write(keyObj, valObj);
			
			//新增活跃下载人数
			keyFields = new String[]{
					appId,
					appVer,
					platform,
					channel,
					country,
					province,
					Constants.PLAYER_TYPE_ONLINE
			};
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(one);
			
			keyObj.setSuffix(Constants.SUFFIX_CHANNEL_PLAYER_DLD_NUM);
			context.write(keyObj, valObj);
			if("Y".equals(isNewPlayer)){
				keyFields[keyFields.length -1] = Constants.PLAYER_TYPE_NEWADD;
				context.write(keyObj, valObj);
			}
			
			//每玩家下载次数分布
			int times = StringUtil.convertInt(downloadTimes, 0);
			int timesRange = EnumConstants.getDownloadTimesRange(times);
			writeLayout(appId, appVer, platform, channel, country, province, isNewPlayer, 
					Constants.DIM_TYPE_DLD_TIMES_DIS+"", timesRange+"", new String[]{downloadTimes}, 
					Constants.SUFFIX_CHANNEL_DLD_TIMES_LAYOUT, context);

			//每资源下载人数分布
			Map<String, Integer> rsDownloadMap = StringUtil.getMapFromJson(downloadRecords, type);
			for(Entry<String, Integer> entry : rsDownloadMap.entrySet()){
				String rsId = entry.getKey();
				//int rsDownloadTime = entry.getValue();
				writeLayout(appId, appVer, platform, channel, country, province, isNewPlayer, 
						Constants.DIM_TYPE_DLD_RESOURCE_DIS+"", rsId+"", one, 
						Constants.SUFFIX_CHANNEL_DLD_NUMS_LAYOUT, context);
			}
		}
	}
	
	private void writeLayout(String appId,
			String appVer,
			String platform,
			String channel,
			String country,
			String province,
			String isNewPlayer,
			String type,
			String vkey,
			String[] value,
			String suffix,
			Context context) throws IOException, InterruptedException{
		
		String[] keyFields = new String[]{
				appId,
				appVer,
				platform,
				channel,
				country,
				province,
				Constants.PLAYER_TYPE_ONLINE,
				type,
				vkey,
		};
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(value);
		
		keyObj.setSuffix(suffix);
		context.write(keyObj, valObj);
		if("Y".equals(isNewPlayer)){
			keyFields[keyFields.length -3] = Constants.PLAYER_TYPE_NEWADD;
			context.write(keyObj, valObj);
		}
	}
}
