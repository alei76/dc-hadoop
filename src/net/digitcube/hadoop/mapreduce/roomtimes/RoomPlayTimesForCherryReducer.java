package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 */

public class RoomPlayTimesForCherryReducer extends Reducer<TmpOutFieldsBaseModel, IntWritable, TmpOutFieldsBaseModel, NullWritable> {
	TmpOutFieldsBaseModel redVal = new TmpOutFieldsBaseModel();
	@Override
	protected void reduce(TmpOutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		String[] outFields = new String[key.getOutFields().length + 1];
		for(int i=0; i<key.getOutFields().length; i++){
			outFields[i] = key.getOutFields()[i];
		}
		outFields[key.getOutFields().length] = ""+total;
		redVal.setOutFields(outFields);
		redVal.setSuffix(key.getSuffix());
		context.write(redVal, NullWritable.get());
	}
}
