package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;

public class TagStayAddRmReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		int totalActivePlayers = 0;
		int totalNewAddPlayers = 0;
		int totalRemovePlayers = 0;
		for(OutFieldsBaseModel val : values){
			int i = 0;
			String isActive = val.getOutFields()[i++];
			String isNewAdd = val.getOutFields()[i++];
			String isRemove = val.getOutFields()[i++];
			if(Boolean.valueOf(isActive)){
				totalActivePlayers++;
			}
			if(Boolean.valueOf(isNewAdd)){
				totalNewAddPlayers++;
			}
			if(Boolean.valueOf(isRemove)){
				totalRemovePlayers++;
			}
		}
		
		String[] valFields = new String[]{
				totalActivePlayers+"",
				totalNewAddPlayers+"",
				totalRemovePlayers+""
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_TAG_STAY_ADD_RM_SUM);
		context.write(key, valObj);
	}
	
}
