package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class GuanKaSumNewReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		if(Constants.SUFFIX_GUANKA_FAIL_REASON_DIST.equals(key.getSuffix())){ // 关卡失败原因分布
			int i = 0;
			String[] keyFields = key.getOutFields();
			String appId = keyFields[i++];
			String platform = keyFields[i++];
			String channel = keyFields[i++];
			String gameServer = keyFields[i++];
			String playerType = keyFields[i++];				
			String guankaId = keyFields[i++];
			String failPoint = keyFields[i++];
			int totalNum = 0;
			int totalTimes = 0;
			for(OutFieldsBaseModel val:values){
				String[] vals = val.getOutFields();
				totalTimes +=StringUtil.convertInt(vals[0], 0); // 次数
				totalNum++; // 人数
			}
			// 通过关失败原因分布-人数
			String[] keyArr = {appId,platform,channel,gameServer,playerType,guankaId};
			key.setOutFields(keyArr);
			key.setSuffix(Constants.SUFFIX_GUANKA_STAT);// 统一后缀（方便入库）
			String[] valArr = {Constants.DIMENSION_GUANKA_FAIL_REASON_NUM, failPoint, totalNum + ""};
			valObj.setOutFields(valArr);
			context.write(key, valObj);
			// 通过关失败原因分布-次数
			valArr = new String[]{Constants.DIMENSION_GUANKA_FAIL_REASON_TIMES, failPoint, totalTimes + ""};
			valObj.setOutFields(valArr);
			context.write(key, valObj);
			
		}else{
			int totalPlayerNum = 0;
			int beginTimes = 0;
			int successTimes = 0;
			int failedTimes = 0;
			int failedExitTimes = 0;
			long totalDuration = 0;
			long succDuration = 0;
			long failDuration = 0;
			
			for(OutFieldsBaseModel val : values){
				
				totalPlayerNum++; //玩家数量加 1
				
				String[] vals = val.getOutFields();
				int i = 0;
				beginTimes += StringUtil.convertInt(vals[i++], 0);
				successTimes += StringUtil.convertInt(vals[i++], 0);
				failedTimes += StringUtil.convertInt(vals[i++], 0);
				failedExitTimes += StringUtil.convertInt(vals[i++], 0);
				
				totalDuration += StringUtil.convertLong(vals[i++], 0);
				succDuration += StringUtil.convertLong(vals[i++], 0);
				failDuration += StringUtil.convertLong(vals[i++], 0);
				
			}
			
			//输出各项统计结果
			key.setSuffix(Constants.SUFFIX_GUANKA_STAT);
			String[] outFields = null;
			//A. 人数统计
			if(totalPlayerNum > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_PLAYER_NUM,
						Constants.DIMENSION_GUANKA_PLAYER_NUM_total,
						totalPlayerNum+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//B. 开始次数
			if(beginTimes > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_PLAY_TIMES,
						Constants.DIMENSION_GUANKA_PLAY_TIMES_total,
						beginTimes+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//C. 成功次数
			if(successTimes > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_PLAY_TIMES,
						Constants.DIMENSION_GUANKA_PLAY_TIMES_succ,
						successTimes+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//D. 失败次数
			if(failedTimes > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_PLAY_TIMES,
						Constants.DIMENSION_GUANKA_PLAY_TIMES_fail,
						failedTimes+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//E. 失败退出次数
			if(failedExitTimes > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_PLAY_TIMES,
						Constants.DIMENSION_GUANKA_PLAY_TIMES_exit,
						failedExitTimes+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//F. 总时长
			if(totalDuration > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_DUARTION,
						Constants.DIMENSION_GUANKA_DUARTION_total,
						totalDuration+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//G. 成功时长
			if(succDuration > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_DUARTION,
						Constants.DIMENSION_GUANKA_DUARTION_succ,
						succDuration+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
			
			//H. 失败时长
			if(failDuration > 0){
				outFields = new String[]{
						Constants.DIMENSION_GUANKA_DUARTION,
						Constants.DIMENSION_GUANKA_DUARTION_fail,
						failDuration+""
				};
				valObj.setOutFields(outFields);
				context.write(key, valObj);
			}
		}
	}
}
