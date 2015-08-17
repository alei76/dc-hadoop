package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;


public class H5OnlineDayForPlayerDeviceReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Set<String> ipSet = new HashSet<String>();
	private Map<Integer, Integer> loginTimeMap = new HashMap<Integer, Integer>();


	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		ipSet.clear();
		loginTimeMap.clear();
		String playerType = null;
		String brand = null;
		String operSystem = null;
		String resolution = null;
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			int i = 0;
			String playerTypeTemp = arr[i++];		
			if(null == playerType){
				playerType = playerTypeTemp;
			}			
			String brandtemp = arr[i++];
			if(null == brand){
				brand = brandtemp;
			}
			String operSystemtemp = arr[i++];
			if(null == operSystem){
				operSystem = operSystemtemp;
			}
			String resolutiontemp = arr[i++];
			if(null == resolution){
				resolution = resolutiontemp;
			}
		}	
		
		String[] valFields = new String[]{
				playerType,
				brand,
				operSystem,
				resolution
		};
		valObj.setOutFields(valFields);
		
		key.setSuffix(Constants.SUFFIX_H5_ONLINEDAY_FOR_PLAYER_DEVICE);
		context.write(key, valObj);
	}
}
