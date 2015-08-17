package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rickpan
 * @version 1.0 
 * <br>
 * 输入: 
 * key : 
 * 		APPID,Platform,GameServer
 * value : 
 * 		onlineCount(在线人数),['CNT','ACU','PCU']
 * 
 * 输出： 
 * APPID,Platform,GameServer,sum(onlineCount),avg(sum(onlineCount)),max(onlineCount)
 * 		
 */

public class AcuPcuCntSumReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel reduceValObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		int hourOnlineCount = 0;
		int avgAcuCount = 0;
		int maxPcuCount = 0;
		
		for (OutFieldsBaseModel val : values) {
			String[] valArr = val.getOutFields();
			String onlineCount = valArr[0];
			int currentCount = StringUtil.convertInt(onlineCount, 0);
			String dataFalg = valArr[1];
			if(Constants.DATA_FLAG_CNT.equals(dataFalg)){
				hourOnlineCount += currentCount; 
			}else if(Constants.DATA_FLAG_ACU.equals(dataFalg)){
				avgAcuCount += currentCount;
				maxPcuCount = currentCount > maxPcuCount ? currentCount : maxPcuCount;
			}else if(Constants.DATA_FLAG_PCU.equals(dataFalg)){
				maxPcuCount = currentCount > maxPcuCount ? currentCount : maxPcuCount;
			}
		}
		avgAcuCount = avgAcuCount / 12;
		
		key.setSuffix(Constants.SUFFIX_ACU_PCU_CNT_SUM);
		
		String[] valFields = new String[]{""+hourOnlineCount, ""+avgAcuCount, ""+maxPcuCount};
		reduceValObj.setOutFields(valFields);
		context.write(key, reduceValObj);
	}

}
