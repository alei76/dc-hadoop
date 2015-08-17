package net.digitcube.hadoop.mapreduce.event;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * see @EventStatisticsMapper
 * 
 */

public class EventStatisticsReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private BloomFilter<String> bloom = null;
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 1000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.000001);
		bloom = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}


	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		//reset bloom and other counts for every key
		bloom.clear();
		int eventCount = 0;
		int uidCount = 0;
		long duration = 0;
		
		if(Constants.SUFFIX_EVENT_STAT.equals(key.getSuffix())){  //事件统计
			for(OutFieldsBaseModel val : values){
				
				String entCount = val.getOutFields()[0];
				String uid = val.getOutFields()[1];
				String dur = val.getOutFields()[2];
				
				eventCount += StringUtil.convertInt(entCount, 0);
				duration += StringUtil.convertLong(dur, 0);
				
				if(!bloom.contains(uid)){
					// UID 不存在，把 UID 添加到集合同时 UID 计数加 1 
					bloom.add(uid);
					uidCount++;
				}
			}
			
			//时长：毫秒-->秒
			//20140521:确认 duration 是秒，所以不用再除以 1000 
			//duration = duration/1000;
			
			String[] outFields = new String[]{""+eventCount, ""+uidCount, ""+duration};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
			
			// 如果是升级事件,则单独输出一份，用于升级时长统计 @UpgradeTimeStatMapper
			/*
			 * 升级事件日志不再从事件统计中输出，由 EventSeparatorMapper 中分离得到
			 * String eventId = key.getOutFields()[key.getOutFields().length - 1];
			if(eventId.contains(Constants.EVENT_ID_LEVEL_UP)){
				key.setSuffix(Constants.SUFFIX_EVENT_LEVEL_UP);
				valObj.setOutFields(new String[]{""+duration});
				context.write(key, valObj);
			}*/
		}else if(Constants.SUFFIX_EVENT_ATTR_STAT.equals(key.getSuffix())){	//事件属性统计
			
			for(OutFieldsBaseModel val : values){
				eventCount++;
				String uid = val.getOutFields()[0];
				if(!bloom.contains(uid)){
					// UID 不存在，把 UID 添加到集合同时 UID 计数加 1
					bloom.add(uid);
					uidCount++;
				}
			}
			
			String[] outFields = new String[]{""+eventCount, ""+uidCount};
			valObj.setOutFields(outFields);
			context.write(key, valObj);
		}
	}
}
