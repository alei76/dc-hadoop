package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.BloomFilter;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageAndModuleRollingReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private BloomFilter<String> bloom = null;
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int expectedNumberOfElements = conf.getInt("event.bloom.inputsize", 10000000);
		String probability = conf.get("event.bloom.inputsize");
		double falsePositiveProbability = StringUtil.convertDouble(probability, 0.0000001);
		//expectedNumberOfElements=1kw, falsePositiveProbability=0.0000001 占不到 50MB 内存
		bloom = new BloomFilter<String>(falsePositiveProbability, expectedNumberOfElements);
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		if(6 == key.getOutFields().length){//页面与功能访问次数及时长
			bloom.clear();
			int totalViewCount=0;
			long totalDuration=0;
			int totalUidCount=0;
			for(OutFieldsBaseModel val : values){
				String duration = val.getOutFields()[0];
				String uid = val.getOutFields()[1];
				totalViewCount++;
				totalDuration += StringUtil.convertInt(duration, 0);
				if(!bloom.contains(uid)){
					// UID 不存在，把 UID 添加到集合同时 UID 计数加 1 
					bloom.add(uid);
					totalUidCount++;
				}
			}
			//访问次数
			String[] valFields = new String[]{
					Constants.DIMENSION_APP_COUNT,
					totalViewCount+""
			};
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(key, valObj);
			//访问时长
			valFields = new String[]{
					Constants.DIMENSION_APP_DURATION,
					totalDuration+""
			};
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(key, valObj);
			//访问用户数（这里指设备数）
			valFields = new String[]{
					Constants.DIMENSION_APP_UID_COUNT,
					totalUidCount+""
			};
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(key, valObj);
			
		}else if(5 == key.getOutFields().length){//滚存及流失玩家最后使用页面与功能
			String[] roll = null;
			String[] lost = null;
			String[] page = null;
			String[] module = null;
			for(OutFieldsBaseModel val : values){
				String[] fields = val.getOutFields();
				if(PageAndModuleRollingMapper.DATA_FLAG_ROLL.equals(val.getSuffix())){
					roll = fields;
				}else if(PageAndModuleRollingMapper.DATA_FLAG_USERLOST.equals(val.getSuffix())){
					lost = fields;
				}else if(PageAndModuleRollingMapper.DATA_FLAG_PAGE.equals(val.getSuffix())){
					if(null == page){
						page = fields;
					}else{
						String viewTime1 = page[1];
						String viewTime2 = fields[1];
						//找出时间最大，即最后使用页面
						if(viewTime2.compareTo(viewTime1) > 0){
							page = fields;
						}
					}
					
				}else if(PageAndModuleRollingMapper.DATA_FLAG_MODULE.equals(val.getSuffix())){
					if(null == module){
						module = fields;
					}else{
						String viewTime1 = module[1];
						String viewTime2 = fields[1];
						//找出时间最大，即最后使用功能
						if(viewTime2.compareTo(viewTime1) > 0){
							module = fields;
						}
					}
				}
			}
			
			//如果只有流失用户则跳过
			if(null == roll && null == page && null == module){
				return;
			}
			
			//A. 输出滚存
			if(null == roll){//新用户
				roll = new String[4];
				
				//最后浏览页面和时间
				if(null == page){
					roll[0] = "-"; //lastViewPage
					roll[1] = "0"; //lastViewPageTime
				}else{
					roll[0] = page[0]; //lastViewPage
					roll[1] = page[1]; //lastViewPageTime
				}
				
				//最后使用功能和时间
				if(null == module){
					roll[2] = "-"; //lastUseModule
					roll[3] = "0"; //lastUseModuleTime
				}else{
					roll[2] = module[0]; //lastUseModule
					roll[3] = module[1]; //lastUseModuleTime
				}
				
			}else{//旧用户
				if(null != page){
					roll[0] = page[0]; //lastViewPage
					roll[1] = page[1]; //lastViewPageTime
				}
				if(null != module){
					roll[2] = module[0]; //lastUseModule
					roll[3] = module[1]; //lastUseModuleTime
				}
			}
			
			valObj.setOutFields(roll);
			key.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_ROLL);
			context.write(key, valObj);
			
			//B. 输出流失用户最后使用页面和功能
			if(null != lost){
				String[] userLost = new String[3];
				userLost[0] = lost[0]; // lostType
				userLost[1] = roll[0]; // lastViewPage
				userLost[2] = roll[2]; // lastUseModule
				
				valObj.setOutFields(userLost);
				key.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_4_LOSTUSER);
				context.write(key, valObj);
			}
		}
	}
	
	public static void main(String[] args){
		System.out.println((byte)'-');
		System.out.println((byte)'0');
		System.out.println((byte)'1');
		System.out.println((byte)'A');
		System.out.println((byte)'a');
	}
}
