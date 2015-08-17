package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 *  下载过程分析
 *  
 *  下载开始次数
 *  key:
 *  { appID, platForm, channel, gameServerReal, netType, appName,fileType, category, appVer, appSize };
 *  Suffix(Constants.SUFFIX_DOWNLOAD_TIMES);
 *  value:one
 *  下载成功次数
 *  key:
 *  {appID, platForm, channel, gameServerReal, netType, appName,fileType, category, appVer, appSize};
 *  Suffix(Constants.SUFFIX_DOWNLOAD_SUCCESS_TIMES);
 *  value:one
 *  下载成功耗时
 *  key:
 *  {appID, platForm, channel, gameServerReal, netType, appName,fileType, category, appVer, appSize};
 *  Suffix(Constants.SUFFIX_DOWNLOAD_COST);
 *  value:one
 *  下载失败原因
 *  key:
 *  { appID, platForm, channel, gameServerReal, reason};
 * 	Suffix(Constants.SUFFIX_DOWNLOAD_FAIL_REASON);
 *  value: one
 *  下载中断原因
 * key:
 *  { appID, platForm, channel, gameServerReal, reason};
 * 	Suffix(Constants.SUFFIX_DOWNLOAD_INTERRUPT_REASON);
 *  value: one
 *  
 *  取出 @EventSeparatorMapper 分离出来的 自定义事件
 *    
 *  下载开始DESelf_APP_DOWNLOAD_START
 *  下载完成DESelf_APP_DOWNLOAD_COMPLETE
 *  下载中断DESelf_APP_DOWNLOAD_INTERRUPT
 *  
 *  
 *  @author Ivan     <br>
 *  @date 2014-6-6         <br>
 *  @version 1.0
 * <br>
 */
public class AppDownloadMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private String fileSuffix;
	private IntWritable one = new IntWritable(1);
	private IntWritable intWritable = new IntWritable();

	/*
	 * 20150202：下载耗时取 60w 的原始数据训练，得到下面结果
	 * 耗时(s)	覆盖数据量
	 * [1,1145]	90%
	 * [1,1717]	92%
	 * [1,2575]	93%
	 * [1,4004]	94%
	 * [1,15725] 95%(异常)
	 * 
	 * 由此下载耗时最大取  2 小时，基本可以覆盖近 95% 的有效数据
	 * 大于两小时的视为异常耗时丢弃，如 95% 这个点耗时是 15725s, 96%、97% 则更大
	 */
	private static final int DOWNLOAD_TIME_MAX = 3600 * 2;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = new EventLog(arr);
		// 下载开始，可以求出 应用的下载次数
		String appID = eventLog.getAppID();
		String platForm = eventLog.getPlatform();
		String channel = eventLog.getChannel();
		String gameServerReal = eventLog.getGameServer();
		String netType = eventLog.getNetType();

		if (fileSuffix.endsWith(Constants.DESELF_APP_DOWNLOAD_START)) {
			// 先取出相关属性
			String appName = eventLog.getArrtMap().get("appName");
			String fileType = eventLog.getArrtMap().get("fileType");
			String category = eventLog.getArrtMap().get("category");
			String appVer = eventLog.getArrtMap().get("appVer");
			String appSize = eventLog.getArrtMap().get("appSize");
			if (appName == null || fileType == null || category == null || appVer == null || appSize == null) {
				return;
			}
			// appSizeRage 范围
			int sizeRange = EnumConstants.getAppSizeRange(StringUtil.convertLong(appSize, 0));

			// 真实区服
			String[] outputKeyArr = new String[] { appID, platForm, channel, gameServerReal, netType, appName,
					fileType, category, appVer, "" + sizeRange };
			outputKey.setOutFields(outputKeyArr);
			outputKey.setSuffix(Constants.SUFFIX_DOWNLOAD_TIMES);
			context.write(outputKey, one);
			// 全区服
			outputKeyArr[3] = MRConstants.ALL_GAMESERVER;
			context.write(outputKey, one);
		} else if (fileSuffix.endsWith(Constants.DESELF_APP_DOWNLOAD_COMPLETE)) {
			// 下载结束，可以求出成功下载次数，耗时情况
			String appName = eventLog.getArrtMap().get("appName");
			String fileType = eventLog.getArrtMap().get("fileType");
			String category = eventLog.getArrtMap().get("category");
			String appVer = eventLog.getArrtMap().get("appVer");
			String appSize = eventLog.getArrtMap().get("appSize");
			if (appName == null || fileType == null || category == null || appVer == null || appSize == null) {
				return;
			}
			// appSizeRage 范围
			int sizeRange = EnumConstants.getAppSizeRange(StringUtil.convertLong(appSize, 0));

			// 下载结果 true 成功，false 失败
			String result = eventLog.getArrtMap().get("result");
			if ("1".equals(result)) {
				int time = eventLog.getDuration();
				// 下载耗时大于 DOWNLOAD_TIME_MAX 的数据视为异常数据丢弃
				if(time > DOWNLOAD_TIME_MAX){
					return;
				}
				
				// 下载成功，记录成功次数
				// 真实区服
				String[] outputKeyArr = new String[] { appID, platForm, channel, gameServerReal, netType, appName,
						fileType, category, appVer, "" + sizeRange };
				outputKey.setOutFields(outputKeyArr);
				outputKey.setSuffix(Constants.SUFFIX_DOWNLOAD_SUCCESS_TIMES);
				context.write(outputKey, one);
				// 全区服
				outputKeyArr[3] = MRConstants.ALL_GAMESERVER;
				context.write(outputKey, one);
				// 下载成功纪录时间
				outputKeyArr = new String[] { appID, platForm, channel, gameServerReal, netType, appName, fileType,
						category, appVer, "" + sizeRange };
				outputKey.setOutFields(outputKeyArr);
				outputKey.setSuffix(Constants.SUFFIX_DOWNLOAD_COST);
				intWritable.set(time);
				context.write(outputKey, intWritable);
				// 全区服
				outputKeyArr[3] = MRConstants.ALL_GAMESERVER;
				context.write(outputKey, intWritable);
			} else {
				// 下载失败，记录失败原因
				// 真实区服reason
				String reason = eventLog.getArrtMap().get("reason");
				if (reason == null) {
					return;
				}
				String[] outputKeyArr = new String[] { appID, platForm, channel, gameServerReal, reason };
				outputKey.setOutFields(outputKeyArr);
				outputKey.setSuffix(Constants.SUFFIX_DOWNLOAD_FAIL_REASON);
				context.write(outputKey, one);
				// 全区服
				outputKeyArr[3] = MRConstants.ALL_GAMESERVER;
				context.write(outputKey, one);
			}

		} else if (fileSuffix.endsWith(Constants.DESELF_APP_DOWNLOAD_INTERRUPT)) {
			// 下载中断、中断原因
			String reason = eventLog.getArrtMap().get("reason");
			if (reason == null) {
				return;
			}
			String[] outputKeyArr = new String[] { appID, platForm, channel, gameServerReal, reason };
			outputKey.setOutFields(outputKeyArr);
			outputKey.setSuffix(Constants.SUFFIX_DOWNLOAD_INTERRUPT_REASON);
			context.write(outputKey, one);
			// 全区服
			outputKeyArr[3] = MRConstants.ALL_GAMESERVER;
			context.write(outputKey, one);
		}
	}
}
