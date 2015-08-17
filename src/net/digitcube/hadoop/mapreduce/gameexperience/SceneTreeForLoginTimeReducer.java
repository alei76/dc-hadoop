package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SceneTreeForLoginTimeReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> {
	private StringBuilder sb = new StringBuilder();
	private TreeMap<String, String> viewTimeMap = new TreeMap<String, String>(); 
	private Text valObj = new Text();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		valObj.clear();
		viewTimeMap.clear();
		sb.delete(0, sb.length());
		
		for(OutFieldsBaseModel val : values){
			String viewTime = val.getOutFields()[0];
			String sceneName = val.getOutFields()[1];
			String duration = val.getOutFields()[2];
			viewTimeMap.put(viewTime, sceneName + ":" + duration);
		}
		
		Set<String> keySet = viewTimeMap.keySet();
		for(String keyStr : keySet){
			sb.append(viewTimeMap.get(keyStr)).append(",");
		}
		
		valObj.set(sb.toString());
		
		key.setSuffix(Constants.SUFFIX_APP_SCENETREE_4_LOGINTIME);
		context.write(key, valObj);
	}

}
