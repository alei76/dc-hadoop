package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoLog2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CHUserInfoReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		//去重，取激活时间最早的记录
		UserInfoLog2 tmpInfo = null;
		for(OutFieldsBaseModel val : values){
			String[] paramArr = val.getOutFields();
			UserInfoLog2 curInfo = new UserInfoLog2(paramArr);
			if(null == tmpInfo){
				keyObj.setOutFields(val.getOutFields());
				tmpInfo = curInfo;
			}else if(curInfo.getActTime() < tmpInfo.getActTime()){
				keyObj.setOutFields(val.getOutFields());
				tmpInfo = curInfo;
			}
		}
		
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_USERINFO);
		context.write(keyObj, NullWritable.get());
	}
}
