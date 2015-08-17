package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
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
 * 
 * 输入：
 * Source 和下载自定义事件 DESelf_APP_VIEW、DESelf_APP_DOWNLOAD
 * 主要逻辑： 统计  APP 的浏览次数和下载次数
 *
 * Reduce 输出:
 * appId, platform, channel, gameServer, type[view|download], fileType, category, fileName, srcName, times
 */
public class AppViewAndDownloadMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable(1);
	
	private String inputFileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//
		keyObj.reset();
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog event = new EventLog(arr);
		
		String fileType = event.getArrtMap().get("fileType");
		String category = event.getArrtMap().get("category");
		String fileName = event.getArrtMap().get("fileName");
		String srcName = event.getArrtMap().get("srcName");
		
		if(StringUtil.isEmpty(fileType)||StringUtil.isEmpty(category)
				||StringUtil.isEmpty(fileName)||StringUtil.isEmpty(srcName)){
			return;
		}
		
		String[] keyFields = null;
		if(inputFileName.endsWith(Constants.DESelf_APP_VIEW)){
			keyFields = new String[]{
					event.getAppID(),
					event.getPlatform(),
					event.getChannel(),
					event.getGameServer(),
					Constants.DIMENSION_APP_VIEW,
					fileType,
					category,
					fileName,
					srcName
			};
		}else if(inputFileName.endsWith(Constants.DESelf_APP_DOWNLOAD)){
			keyFields = new String[]{
					event.getAppID(),
					event.getPlatform(),
					event.getChannel(),
					event.getGameServer(),
					Constants.DIMENSION_APP_DOWNLOAD,
					fileType,
					category,
					fileName,
					srcName
			};
		}
		
		if(null == keyFields){
			return;
		}
		keyObj.setOutFields(keyFields);
		context.write(keyObj, valObj);
		
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		keyObj.setOutFields(keyFields);
		context.write(keyObj, valObj);
	}
}
