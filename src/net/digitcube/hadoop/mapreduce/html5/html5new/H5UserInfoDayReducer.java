package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5UserInfoDayLog;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class H5UserInfoDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		H5UserInfoDayLog h5UserInfoDayLog = null;
		for(OutFieldsBaseModel val : values){
			if(null == h5UserInfoDayLog){
				h5UserInfoDayLog = new H5UserInfoDayLog(val.getOutFields());
			}else{
				H5UserInfoDayLog current = new H5UserInfoDayLog(val.getOutFields());
				//取非空 accountID
				if(!MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(current.getAccountId())){
					h5UserInfoDayLog.setAccountId(current.getAccountId());
				}
				//按字母自然顺序比较，取大的版本号
				if(current.getAppId().compareTo(h5UserInfoDayLog.getAppId())>0){
					h5UserInfoDayLog.setAppId(current.getAppId());
				}
			}
		}
		if(null == h5UserInfoDayLog){
			return;
		}	
		OutFieldsBaseModel randomOne = new OutFieldsBaseModel(h5UserInfoDayLog.toStringArray());
		randomOne.setSuffix(Constants.SUFFIX_H5_NEW_USERINFO_DAY);
		context.write(randomOne, NullWritable.get());		
	}
}
