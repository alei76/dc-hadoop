package net.digitcube.hadoop.tmp;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LostPlayerGuankaLayoutReducer extends Reducer<Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(Text key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String finalLevelId = "";
		int finalTime = 0;
		for(OutFieldsBaseModel val : values){
			String levelId = val.getOutFields()[0];
			int startTime = StringUtil.convertInt(val.getOutFields()[1],0);
			
			if(startTime > finalTime){
				finalLevelId = levelId;
				finalTime = startTime;
			}
		}
		valObj.setOutFields(new String[]{
				key.toString(),
				finalLevelId,
				finalTime+""
		});
		valObj.setSuffix("7Day_Lost_Final_GuanKa");
		context.write(valObj, NullWritable.get());
	}
}
