package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.mapreduce.Reducer;

public class H5PageViewForPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Map<String, Map<String, Integer>> loginTimePVMap = new HashMap<String, Map<String, Integer>>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		loginTimePVMap.clear();
		
		String isNewPlayer = Constants.DATA_FLAG_NO;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			String newPlayer = arr[0];
			String loginTime = arr[1];
			String pvKey = arr[2];
			
			if(Constants.DATA_FLAG_YES.equals(newPlayer)){
				isNewPlayer = Constants.DATA_FLAG_YES;
			}
			
			Map<String, Integer> pvMap = loginTimePVMap.get(loginTime);
			if(null == pvMap){
				pvMap = new HashMap<String, Integer>();
				loginTimePVMap.put(loginTime, pvMap);
			}
			Integer oldCount = pvMap.get(pvKey);
			if(null == oldCount){
				pvMap.put(pvKey, 1);
			}else{
				pvMap.put(pvKey, 1 + oldCount);
			}
		}
		
		int playerTotalPVs = 0;
		int player1ViewPvs = 0;
		for(Map<String, Integer> map : loginTimePVMap.values()){
			playerTotalPVs += map.size();
			
			//如果 1 == map.size()，说明某次登录只访问一个页面，本次登录可记为一次跳出
			if(1 == map.size()){
				player1ViewPvs++;
			}
		}
		
		String pvRecords = StringUtil.getJsonStr(loginTimePVMap);
		
		String[] valFields = new String[]{
				isNewPlayer,
				playerTotalPVs + "",
				player1ViewPvs + "",
				pvRecords
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_PV_FOR_PLAYER);
		
		context.write(key, valObj);
	}
}
