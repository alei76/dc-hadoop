package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyWordSearchStatReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int searchSuccessTimes = 0;
		int searchFailedTimes = 0;
		int searchDownloadTimes = 0;
		for(Text flag : values){
			if(KeyWordSearchStatMapper.KEYWORD_SEARCH_SUCCESS.equals(flag.toString())){
				searchSuccessTimes++;
			}else if(KeyWordSearchStatMapper.KEYWORD_SEARCH_FAILED.equals(flag.toString())){
				searchFailedTimes++;
			}else if(KeyWordSearchStatMapper.KEYWORD_SEARCH_DOWNLOAD.equals(flag.toString())){
				searchDownloadTimes++;
			}
		}
		
		int totalSearchTimes = searchSuccessTimes + searchFailedTimes; 
		String[] valFields = new String[]{
				totalSearchTimes + "",
				searchSuccessTimes + "",
				searchDownloadTimes + ""
		};
		
		key.setSuffix(Constants.SUFFIX_APP_KEYWORD_SEARCH_STAT);
		valObj.setOutFields(valFields);
		
		context.write(key, valObj);
	}
}
