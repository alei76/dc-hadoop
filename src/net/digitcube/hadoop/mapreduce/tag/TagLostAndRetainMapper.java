package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TagLostAndRetainMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {

	public static final String DATA_FLAG_TAG_LOST = "L";
	public static final String DATA_FLAG_TAG_REATIN = "R";
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable(1);
	
	// 当前输入的文件后缀
	private String fileSuffix;
	
	// 取结算日期(数据发生日期)
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		if(context.getConfiguration().getBoolean("is.hour.job", false)){
			//小时任务:结算时间取当天凌晨零点
			statDate = DateUtil.getStatDateForToday(context.getConfiguration());
		}else{
			//天任务:结算时间取昨天凌晨零点
			statDate = DateUtil.getStatDate(context.getConfiguration());
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		if(fileSuffix.endsWith(Constants.SUFFIX_TAG_PLAYER_LOST)){
			String appId = paraArr[i++];
			String appVer = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String parTagName = paraArr[i++];
			String subTagName = paraArr[i++];
			String lostLevel = paraArr[i++];
			int lostDays = StringUtil.convertInt(paraArr[i++], 0);

			String[] keyFields = new String[]{
					statDate+"",
					appId,
					appVer,
					platform,
					channel,
					gameServer,
					parTagName,
					subTagName,
					"0"
			};
			String[] keyFieldsLevel = new String[]{
					statDate+"",
					appId,
					platform,
					channel,
					gameServer,
					parTagName,
					subTagName,
					lostLevel,
					"0"
			};
			
			//+3 日流失
			int dayOffset = 3;
			if((lostDays & 2) > 0){
				int statTime = statDate - dayOffset * 24 * 3600;
				
				// 流失玩家数
				keyFields[0] = statTime+"";
				keyFields[keyFields.length - 1] = dayOffset+"";
				writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_LOST_SUM, context);
				
				// 流失等级分布
				keyFieldsLevel[0] = statTime+"";
				keyFieldsLevel[keyFieldsLevel.length - 1] = dayOffset+"";
				writeResult(keyFieldsLevel, Constants.SUFFIX_TAG_PLAYER_LOST_LEVEL, context);
			}
			//+7 日流失
			dayOffset = 7;
			if((lostDays & 4) > 0){
				int statTime = statDate - dayOffset * 24 * 3600;
				
				// 流失玩家数
				keyFields[0] = statTime+"";
				keyFields[keyFields.length - 1] = dayOffset+"";
				writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_LOST_SUM, context);
				
				// 流失等级分布
				keyFieldsLevel[0] = statTime+"";
				keyFieldsLevel[keyFieldsLevel.length - 1] = dayOffset+"";
				writeResult(keyFieldsLevel, Constants.SUFFIX_TAG_PLAYER_LOST_LEVEL, context);
			}
			//+14 日流失
			dayOffset = 14;
			if((lostDays & 8) > 0){
				int statTime = statDate - dayOffset * 24 * 3600;
				
				// 流失玩家数
				keyFields[0] = statTime+"";
				keyFields[keyFields.length - 1] = dayOffset+"";
				writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_LOST_SUM, context);
				
				// 流失等级分布
				keyFieldsLevel[0] = statTime+"";
				keyFieldsLevel[keyFieldsLevel.length - 1] = dayOffset+"";
				writeResult(keyFieldsLevel, Constants.SUFFIX_TAG_PLAYER_LOST_LEVEL, context);
			}
			//+30 日流失
			dayOffset = 30;
			if((lostDays & 16) > 0){
				int statTime = statDate - dayOffset * 24 * 3600;
				
				// 流失玩家数
				keyFields[0] = statTime+"";
				keyFields[keyFields.length - 1] = dayOffset+"";
				writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_LOST_SUM, context);
				
				// 流失等级分布
				keyFieldsLevel[0] = statTime+"";
				keyFieldsLevel[keyFieldsLevel.length - 1] = dayOffset+"";
				writeResult(keyFieldsLevel, Constants.SUFFIX_TAG_PLAYER_LOST_LEVEL, context);
			}
			
		}else if(fileSuffix.endsWith(Constants.SUFFIX_TAG_PLAYER_RETAIN)){
		
			String appId = paraArr[i++];
			String appVer = paraArr[i++];
			String platform = paraArr[i++];
			String channel = paraArr[i++];
			String gameServer = paraArr[i++];
			String accountId = paraArr[i++];
			String parTagName = paraArr[i++];
			String subTagName = paraArr[i++];
			String playerType = Constants.PLAYER_TYPE_ONLINE;
			String[] keyFields = new String[]{
					statDate+"",
					appId,
					appVer,
					platform,
					channel,
					gameServer,
					parTagName,
					subTagName,
					playerType,
					"0",
			};
			
			for(int j=1; j<31; j++){
				int tmpPlayerType = StringUtil.convertInt(paraArr[i + j - 1], 0);
				if(0 == tmpPlayerType){
					continue;
				}
				
				int statTime = statDate - j * 24 * 3600;
				keyFields[0] = statTime + "";
				// 第 N 天的留存
				keyFields[keyFields.length - 1] = j + "";
				
				// 活跃标签玩家留存
				keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_ONLINE;
				writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_RETAIN_SUM, context);
				// 新标签玩家留存
				if((tmpPlayerType & 2) > 0){
					keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_NEWADD;
					writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_RETAIN_SUM, context);
				}
				// 付费标签玩家留存
				if((tmpPlayerType & 4) > 0){
					keyFields[keyFields.length - 2] = Constants.PLAYER_TYPE_PAYMENT;
					writeResult(keyFields, Constants.SUFFIX_TAG_PLAYER_RETAIN_SUM, context);
				}
			}
		}
	}
	
	private void writeResult(String[] keyFields, String suffix, Context context) throws IOException, InterruptedException{
		keyObj.setOutFields(keyFields);
		keyObj.setSuffix(suffix);
		context.write(keyObj, valObj);
	}
}
