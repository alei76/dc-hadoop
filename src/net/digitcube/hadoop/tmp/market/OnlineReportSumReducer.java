package net.digitcube.hadoop.tmp.market;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlineReportSumReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, TmpOutFieldsBaseModel, NullWritable> {

	TmpOutFieldsBaseModel valObj = new TmpOutFieldsBaseModel();
	private TreeMap<Integer, Long> hourCount = new TreeMap<Integer, Long>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		valObj.setSuffix(key.getSuffix());
		
		if(OnlineReportMapper.HOUR_OL_NUM.equals(key.getSuffix())){
			//clear map
			hourCount.clear();
			
			for(OutFieldsBaseModel val : values){
				long totalPlayerCount = StringUtil.convertLong(val.getOutFields()[0],0);
				int hour  = StringUtil.convertInt(val.getOutFields()[1],-1);
				if(-1 == hour){
					continue;
				}
				
				Long count = hourCount.get(hour);
				if(null == count){
					hourCount.put(hour, totalPlayerCount);
				}else{
					hourCount.put(hour, count + totalPlayerCount);
				}
			}
			
			Set<Entry<Integer, Long>> set = hourCount.entrySet();
			for(Entry<Integer, Long> entry : set){
				long avgSum = entry.getValue() / 30;
				String[] valFileds = new String[]{
						entry.getKey().toString(),
						entry.getValue().toString(),
						avgSum + ""
				};
				valObj.setOutFields(valFileds);
				context.write(valObj, NullWritable.get());
			}
			
		}else{
			long totalAvgSum = 0;
			for(OutFieldsBaseModel val : values){
				totalAvgSum += StringUtil.convertLong(val.getOutFields()[0], 0);
			}
			
			long avgSum = totalAvgSum / 30; 
			String[] valFields = new String[]{
					totalAvgSum + "",
					avgSum + ""	
			};
			
			valObj.setOutFields(valFields);
			context.write(valObj, NullWritable.get());
		}
	}
}
