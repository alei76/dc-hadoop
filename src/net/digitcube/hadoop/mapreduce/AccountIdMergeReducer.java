package net.digitcube.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.HeaderLog;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AccountIdMergeReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {
	//一个 UID 下对应的所有 account id
	private Set<String> accIdSet = new HashSet<String>();
	//一个 UID 下一天内应该不会有太多数据
	private Set<OutFieldsBaseModel> logSet = new HashSet<OutFieldsBaseModel>();
	//UID
	private String uid = null;
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		accIdSet.clear();
		logSet.clear();
		uid = key.getOutFields()[1];
		
		for(OutFieldsBaseModel val : values){
			accIdSet.add(val.getSuffix());
			logSet.add(new OutFieldsBaseModel(val.getOutFields()));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String targetAccoutId2Merge = null;
		//A. 
		//找到任意一个不为 UID 和不为 '-' 的 account id
		//用于替换 logSet 中 account id 为  UID 或 '-' 的记录
		for(String accId : accIdSet){
			if(!MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(accId) && !uid.equals(accId)){
				targetAccoutId2Merge = accId;
				break;
			}
		}
		//B.
		//如果 targetAccoutId2Merge 为 null，说明 uid 要么为 UID 要么为 '-'
		//如果 accIdSet 存在 UID，则优先取 UID，否则取 '-'
		if(accIdSet.contains(uid)){
			targetAccoutId2Merge = uid;
		}else{
			//此时说明SDK 上报的日志中没有设置 account id
			targetAccoutId2Merge = MRConstants.INVALID_PLACE_HOLDER_CHAR;
		}
		
		//C.
		//替换目标 account id 并输出
		try{
			String fileName = context.getConfiguration().get("dc.log.name");
			String realClass = context.getConfiguration().get("dc.log.model.class");
			Constructor constructor = Class.forName(realClass).getConstructor(String[].class);
			for(OutFieldsBaseModel val : logSet){
				HeaderLog log = (HeaderLog)(constructor.newInstance((Object)val.getOutFields()));
				String accId = log.getAccountID();
				//如果 accId 为  UID 或 '-'，则替换为 targetAccoutId2Merge
				if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(accId) || uid.equals(accId)){
					log.setAccountID(targetAccoutId2Merge);
					val.setOutFields(log.toStringArr());
				}
				val.setSuffix(fileName);
				context.write(val, NullWritable.get());
			}
		}catch(Throwable t){
			throw new RuntimeException(t);
		}
	}
}
