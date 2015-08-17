package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;

public class H5VirusSpreadForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String parentAccountId = null;
		String totalLoginTimes = null;
		String totalOnlineTime = null;
		String ipRecords = null;
		String totalPVs = null;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			if(H5VirusSpreadForPlayerMapper.FLAG_ONLINE.equals(val.getSuffix())){
				totalLoginTimes = arr[0];
				totalOnlineTime = arr[1];
				ipRecords = arr[2];
			}else if(H5VirusSpreadForPlayerMapper.FLAG_PV.equals(val.getSuffix())){
				totalPVs = arr[0];
			}else if(H5VirusSpreadForPlayerMapper.FLAG_USERINFO.equals(val.getSuffix())){
				parentAccountId = arr[0];
			}
		}
		
		//没有关联到注册日志，说明该玩家不是新增玩家，病毒传播主要是从注册日志中进行统计
		//所以没有关联到注册日志时，直接返回
		if(null == parentAccountId){
			return;
		}
		
		String[] valFields = new String[]{
				parentAccountId,
				null == totalLoginTimes ? "0" : totalLoginTimes,
				null == totalOnlineTime ? "0" : totalOnlineTime,
				null == totalPVs ? "0" : totalPVs,
				null == ipRecords ? "[]" : ipRecords
		}; 
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_VIRUS_SPREAD_FOR_PLAYER);
		
		context.write(key, valObj);
	}
}
