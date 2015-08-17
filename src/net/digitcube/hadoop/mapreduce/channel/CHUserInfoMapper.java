package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.UserInfoLog2;
import net.digitcube.hadoop.util.FieldValidationUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：当天 24 个时间片的激活日志
 * 主要逻辑： 按 UID 进行去重，取激活时间最早的值
 */
public class CHUserInfoMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paramArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoLog2 userInfoLog2 = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			userInfoLog2 = new UserInfoLog2(paramArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		//验证appId长度 并修正appId  add by mikefeng 20141010
		/*if(!FieldValidationUtil.validateAppIdLength(userInfoLog2.getAppID())){
			return;
		}*/
		
		if(userInfoLog2.getActTime() > 0){
			String[] keyFields = new String[]{
					userInfoLog2.getAppID(),
					userInfoLog2.getPlatform(),
					userInfoLog2.getUID()
			};
			
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(paramArr);
			
			context.write(keyObj, valObj);
		}
	}
}
