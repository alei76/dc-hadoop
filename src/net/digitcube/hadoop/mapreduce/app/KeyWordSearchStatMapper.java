package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * 输入：
 * Source 和下载自定义事件 DESelf_APP_SOURCE、DESelf_APP_DOWNLOAD
 * 主要逻辑： 
 * 统计搜索关键字的搜索次数、搜索成功次数、搜索下载次数
 *
 * Reduce 输出:
 * appId, platform, channel, gameServer, keyword, totalSearchTimes, searchSuccessTimes, searchDownloadTimes
 */
public class KeyWordSearchStatMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	
	static final String KEYWORD_SEARCH_FAILED = "SF";
	static final String KEYWORD_SEARCH_SUCCESS = "SS";
	static final String KEYWORD_SEARCH_DOWNLOAD = "SD";
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private Text valObj = new Text();
	
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog event = new EventLog(arr);
		
		String keyword = event.getArrtMap().get("keyword");
		String result = event.getArrtMap().get("result");
		
		if(StringUtil.isEmpty(keyword)|| keyword.getBytes("UTF-8").length > 65535){//正常事件，但没有搜索
			return;
		}
		
		String[] keyFields = new String[]{
				event.getAppID(),
				event.getPlatform(),
				event.getChannel(),
				event.getGameServer(),
				keyword
		};
		
		
		if(fileName.endsWith(Constants.DESelf_APP_SOURCE)){
			if("1".equals(result)){
				valObj.set(KEYWORD_SEARCH_SUCCESS);
			}else{
				valObj.set(KEYWORD_SEARCH_FAILED);
			}
		}else if(fileName.endsWith(Constants.DESelf_APP_DOWNLOAD)){
			valObj.set(KEYWORD_SEARCH_DOWNLOAD);
		}
		
		keyObj.setOutFields(keyFields);
		context.write(keyObj, valObj);
		
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		keyObj.setOutFields(keyFields);
		context.write(keyObj, valObj);
	}
}
