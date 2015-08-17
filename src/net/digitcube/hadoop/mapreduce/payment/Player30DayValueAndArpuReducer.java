package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class Player30DayValueAndArpuReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private int[] ltv = new int[30];
	private float[] arpu = new float[30];
	
	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	
	//用于存储新增玩家 N 天后的 DAU，用于辅助计算 DAU
	//该 Map 将存储整个 APP 当天的所有 30 日内新增玩家帐号
	//当某 APP 30 内新增玩家当天 DAU 为 100w 时，该 Map 大概占 64MB 内存（还可接受）
	//如果该 DAU 超过 100w 时，就考虑不能再在内存中计算了
	//需增加多一个步骤的 MR 辅助计算
	private Map<Integer, Set<String>> daysAccountNum = new HashMap<Integer, Set<String>>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		
		reset();
		daysAccountNum.clear();
		if(Constants.SUFFIX_PLAYER_30DAYVALUE_SUM.equals(key.getSuffix())){
			for(OutFieldsBaseModel val : values){
				int days = StringUtil.convertInt(val.getOutFields()[0], 0);
				int currency = StringUtil.convertInt(val.getOutFields()[1], 0);
				if(days <= 0 || currency <= 0){
					continue;
				}
				
				ltv[days - 1] += currency;
			}
			String[] valFields = new String[30];
			for(int i=0; i<ltv.length; i++){
				valFields[i] = ""+ltv[i];
			}
			redValObj.setOutFields(valFields);
			context.write(key, redValObj);
			
		}else if(Constants.SUFFIX_PLAYER_30DAY_ARPU_SUM.equals(key.getSuffix())){
			for(OutFieldsBaseModel val : values){
				int days = StringUtil.convertInt(val.getOutFields()[0], 0);
				int currency = StringUtil.convertInt(val.getOutFields()[1], 0);
				String accountId = val.getOutFields()[2];
				if(days <= 0){
					continue;
				}
				
				int daysOffSet = days - 1; 
				arpu[daysOffSet] += currency;
				Set<String> accountIdSet = daysAccountNum.get(daysOffSet);
				if(null == accountIdSet){
					accountIdSet = new HashSet<String>();
					daysAccountNum.put(daysOffSet, accountIdSet);
				}
				accountIdSet.add(accountId);
			}
			
			String[] valFields = new String[30];
			for(int day=0; day<arpu.length; day++){
				Set<String> accountIdSet = daysAccountNum.get(day);
				if(null == accountIdSet || 0 == accountIdSet.size()){
					valFields[day] = "" + 0;
				}else{
					// ARPDAU = 当天收入总额除以总活跃人数
					valFields[day] = "" + arpu[day]/accountIdSet.size();
				}
			}
			
			redValObj.setOutFields(valFields);
			context.write(key, redValObj);
		}
	}

	private void reset(){
		for(int i=0; i<30; i++){
			ltv[i] = 0;
			arpu[i] = 0;
		}
	}
	
	public static void main(String[] args){
		System.out.println(1/3f);
	}
}
