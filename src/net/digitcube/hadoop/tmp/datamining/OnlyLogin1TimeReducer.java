package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class OnlyLogin1TimeReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int totalNewPlayer = 0;
		int login1TimePlayer = 0;
		for (IntWritable val : values) {
			totalNewPlayer++;
			if(1 == val.get()){
				login1TimePlayer++;
			}
		}
		
		float login1TimeRate = ((float)login1TimePlayer)/totalNewPlayer;
		String[] valFields = new String[]{
				login1TimeRate + "",
				login1TimePlayer + "",
				totalNewPlayer + ""
		};
		key.setSuffix("LOGIN_1_TIME_PLAYER");
		valObj.setOutFields(valFields);
		context.write(key, valObj);
	}
}
