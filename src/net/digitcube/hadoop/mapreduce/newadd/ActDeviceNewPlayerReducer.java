package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ActDeviceNewPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		OutFieldsBaseModel onlinePlayer = null;
		boolean isDeviceActExist = false;
		for(OutFieldsBaseModel val : values){
			if(val.getSuffix().equals(Constants.DATA_FLAG_FIRST_ONLINE)){
				//找出激活设备中的新增用户（一台激活设备可能有多个新增用户），随机找出一个即可
				if(null == onlinePlayer){
					onlinePlayer = new OutFieldsBaseModel(val.getOutFields());
					onlinePlayer.setSuffix(Constants.SUFFIX_ACT_DEV_FIR_ONLINE);
				}
			}else if(val.getSuffix().equals(Constants.DATA_FLAG_DEVICE_ACT)){
				isDeviceActExist = true;
			}
		}
		
		if(isDeviceActExist && null != onlinePlayer){
			context.write(onlinePlayer, NullWritable.get());
		}
	}
}
