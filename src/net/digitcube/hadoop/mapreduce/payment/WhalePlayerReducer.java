package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class WhalePlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		
		int firstLoginDay = 0;
		int firstPayDay = 0;
		int payAmount = 0;
		int payTimes = 0;
		int onlineDays = 0;
		int onlineTime = 0;
		int gameTimes = 0;
		int level = 0;
		
		int loginTimesToday = 0; 
		int onlineTimeToday = 0; 
		float payAmountToday = 0;
		
		for(OutFieldsBaseModel val : values){
			int i = 0;
			//firstLoginDay:取最小
			int loginDay = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if(0 == firstLoginDay){
				firstLoginDay = loginDay;
			}else if(0 != loginDay){
				firstLoginDay = Math.min(firstLoginDay, loginDay);
			}
			
			//firstPayDay:取最小
			int payDay = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if(0 == firstPayDay){
				firstPayDay = payDay;
			}else if(0 != payDay){
				firstPayDay = Math.min(firstPayDay, payDay);
			}
			
			//payAmount,payTimes:累加
			payAmount += StringUtil.convertInt(val.getOutFields()[i++], 0);
			payTimes += StringUtil.convertInt(val.getOutFields()[i++], 0);
			
			//onlineDays
			int onlineDay = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if(0 == onlineDays){
				onlineDays = onlineDay;
			}else{
				onlineDays = Math.max(onlineDays, onlineDay);
			}
			
			//onlineTime
			int time = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if(0 == onlineTime){
				onlineTime = time;
			}else{
				onlineTime = Math.max(onlineTime, time);
			}
			
			//gameTimes
			gameTimes += StringUtil.convertInt(val.getOutFields()[i++], 0);
			
			//level
			int le = StringUtil.convertInt(val.getOutFields()[i++], 0);
			if(0 == level){
				level = le;
			}else{
				level = Math.max(level, le);
			}
			
			//
			loginTimesToday += StringUtil.convertInt(val.getOutFields()[i++], 0);
			onlineTimeToday += StringUtil.convertInt(val.getOutFields()[i++], 0);
			payAmountToday += StringUtil.convertFloat(val.getOutFields()[i++], 0);
		}
		
		redValObj.setOutFields(new String[]{
				firstLoginDay+"",
				firstPayDay+"",
				payAmount+"",
				payTimes+"",
				onlineDays+"",
				onlineTime+"",
				gameTimes+"",
				level+"",
				loginTimesToday+"",
				onlineTimeToday+"",
				payAmountToday+""
		});
		
		key.setSuffix(Constants.SUFFIX_WHALE_PLAYER);
		context.write(key, redValObj);
	}

}
