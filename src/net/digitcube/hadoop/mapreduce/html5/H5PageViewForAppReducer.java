package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5PageViewForAppReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		int totalPVs = 0;
		int oneViewPvs = 0;
		for (OutFieldsBaseModel val : values){
			String[] arr = val.getOutFields();
			totalPVs += StringUtil.convertInt(arr[0], 0);
			oneViewPvs += StringUtil.convertInt(arr[1], 0);
		}
		
		String[] valFields = new String[]{
				totalPVs + "",
				oneViewPvs + ""
		}; 
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_PV_FOR_APP);
		
		context.write(key, valObj);
	}
}
