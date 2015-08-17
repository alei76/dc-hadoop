package net.digitcube.hadoop.driver;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;

public class PersonReducer extends Reducer<BigFieldsBaseModel, BigFieldsBaseModel, BigFieldsBaseModel, BigFieldsBaseModel> {

	private BigFieldsBaseModel valObj = new BigFieldsBaseModel();
	
	@Override
	protected void reduce(BigFieldsBaseModel key, Iterable<BigFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		int totalCount = 0;
		int totalMoney = 0;
		for(BigFieldsBaseModel val : values){
			String[] arr =  val.getOutFields();
			totalCount += Integer.valueOf(arr[0]);
			totalMoney += Integer.valueOf(arr[1]);
		}
		
		String[] valFields = new String[]{
				totalCount + "",
				totalMoney + ""
		};
		
		valObj.setOutFields(valFields);
		key.setSuffix("BIG_FILEDS");
		
		context.write(key, valObj);
	}
}
