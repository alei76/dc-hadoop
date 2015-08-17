package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class TagOnlineAndPayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	//level<-->playerNum
	private Map<String, Integer> level2NumMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		level2NumMap.clear();
		
		int totalOnlinePlayers = 0;
		int totalLoginTimes = 0;
		int totalOnlineTime = 0;

		int totalPayPlayers = 0;
		int totalPayTimes = 0;
		float totalPayAmount = 0;
		
		for(OutFieldsBaseModel val : values){
			if(TagOnlineAndPayMapper.DATA_FLAG_TAG_ONLINE.equals(val.getSuffix())){
				totalOnlinePlayers++;
				String loginTimes = val.getOutFields()[0];
				String onlineTime = val.getOutFields()[1];
				String level = val.getOutFields()[2];
				totalLoginTimes += StringUtil.convertInt(loginTimes, 0);
				totalOnlineTime += StringUtil.convertInt(onlineTime, 0);
				
				//等级<-->活跃人数
				Integer num = level2NumMap.get(level);
				if(null != null){
					level2NumMap.put(level, 1 + num);
				}else{
					level2NumMap.put(level, 1);
				}
			}else if(TagOnlineAndPayMapper.DATA_FLAG_TAG_PAYMENT.equals(val.getSuffix())){
				totalPayPlayers++;
				String payTimes = val.getOutFields()[0];
				String payAmount = val.getOutFields()[1];
				totalPayTimes += StringUtil.convertInt(payTimes, 0);
				totalPayAmount += StringUtil.convertFloat(payAmount, 0);
			}
		}
		
		String[] valFields = new String[]{
				totalOnlinePlayers+"",
				totalLoginTimes+"",
				totalOnlineTime+"",
				totalPayPlayers+"",
				totalPayTimes+"",
				totalPayAmount+""
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_TAG_OL_PAY_SUM);
		context.write(key, valObj);
		
		//输出等级<-->活跃人数
		for(Entry<String, Integer> entry : level2NumMap.entrySet()){
			valFields = new String[]{
					entry.getKey(),
					entry.getValue()+""
			};
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_TAG_LEVEL_PLAYER_NUM);
			context.write(key, valObj);
		}
	}
	
}
