package net.digitcube.hadoop.mapreduce;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.UserInfoLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ActRegSeparateReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		//去重，取最大激活时间注册时间
		UserInfoLog userInfoLog = null;
		for(OutFieldsBaseModel val : values){
			if(null == userInfoLog){
				userInfoLog = new UserInfoLog(val.getOutFields());
			}else{
				UserInfoLog current = new UserInfoLog(val.getOutFields());
				if(current.getActTime() > userInfoLog.getActTime()){
					userInfoLog.setActTime(current.getActTime());
				}
				if(current.getRegTime() > userInfoLog.getRegTime()){
					userInfoLog.setRegTime(current.getRegTime());
				}
			}
		}
		if(null == userInfoLog){
			return;
		}
		
		OutFieldsBaseModel randomOne = new OutFieldsBaseModel(userInfoLog.toOldVersionArr()); 
		randomOne.setSuffix(key.getSuffix());
		context.write(randomOne, NullWritable.get());
		
		/*String[] userInfoArr = randomOne.getOutFields();
		int actTime = StringUtil.convertInt(userInfoArr[Constants.INDEX_ACTTIME], 0);
		int regTime = StringUtil.convertInt(userInfoArr[Constants.INDEX_REGTIME], 0);
		
		if(0 != actTime){ // 是当前时间片的激活玩家
			randomOne.setSuffix(Constants.SUFFIX_ACT);
			context.write(randomOne, NullWritable.get());
		}
		
		if(0 != regTime){// 注册时间有效，视为当前时间片的注册玩家
			randomOne.setSuffix(Constants.SUFFIX_REG);
			context.write(randomOne, NullWritable.get());
		}*/
	}
}
