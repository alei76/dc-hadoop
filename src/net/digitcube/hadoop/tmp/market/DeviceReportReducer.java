package net.digitcube.hadoop.tmp.market;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class DeviceReportReducer extends Reducer<OutFieldsBaseModel, IntWritable, TmpOutFieldsBaseModel, NullWritable> {

	private TmpOutFieldsBaseModel valObj = new TmpOutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int total = 0;
		for (IntWritable val : values) {
			total += val.get();
		}
		
		String[] valFields = new String[]{
				key.getOutFields()[0],
				total + "",
		};
		valObj.setSuffix(key.getSuffix());
		valObj.setOutFields(valFields);
		context.write(valObj, NullWritable.get());
	}
}
