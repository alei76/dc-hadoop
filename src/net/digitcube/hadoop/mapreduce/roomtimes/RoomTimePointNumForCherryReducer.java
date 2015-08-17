package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

public class RoomTimePointNumForCherryReducer extends Reducer<TmpOutFieldsBaseModel, TmpOutFieldsBaseModel, TmpOutFieldsBaseModel, NullWritable> {
	private TmpOutFieldsBaseModel redVal = new TmpOutFieldsBaseModel();
	private Map<String, Set<String>> timePointAccountSet = new HashMap<String, Set<String>>();
	
	@Override
	protected void reduce(TmpOutFieldsBaseModel key, Iterable<TmpOutFieldsBaseModel> values,
			Context context) throws IOException, InterruptedException {
		timePointAccountSet.clear();
		
		for (TmpOutFieldsBaseModel val : values) {
			String timePoint = val.getOutFields()[0];
			String accountId = val.getOutFields()[1];
			Set<String> set = timePointAccountSet.get(timePoint);
			if(null == set){
				set = new HashSet<String>();
				timePointAccountSet.put(timePoint, set);
			}
			set.add(accountId);
		}
		
		int keyLength = key.getOutFields().length;
		String[] outFields = new String[keyLength + 24];
		for(int i=0; i<keyLength; i++){
			outFields[i] = key.getOutFields()[i];
		}
		for(int i=keyLength; i<outFields.length; i++){
			Set<String> set = timePointAccountSet.get(""+(i-keyLength));
			if(null == set){
				outFields[i] = ""+0;
			}else{
				outFields[i] = ""+set.size();
			}
		}
		redVal.setOutFields(outFields);
		redVal.setSuffix("TimePointPlayerCounts");
		context.write(redVal, NullWritable.get());
	}
}
