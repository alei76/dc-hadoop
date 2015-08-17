package net.digitcube.hadoop.mapreduce.accountmonitor;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AccountNumPerDeviceReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel accInfo = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		int accountNum = 0;
		for(OutFieldsBaseModel val : values){
			int tmpAccNum = StringUtil.convertInt(val.getOutFields()[0], 0);
			if(tmpAccNum >= accountNum){
				accountNum = tmpAccNum;
				accInfo.setOutFields(val.getOutFields());
			}
		}
		
		String[] outFields = new String[]{key.getOutFields()[0], //appid
												key.getOutFields()[1], //platform
												accInfo.getOutFields()[1], //channel
												accInfo.getOutFields()[2], //gameserver
												accInfo.getOutFields()[0]};
		
		accInfo.setOutFields(outFields);
		accInfo.setSuffix(Constants.SUFFIX_ACCOUNT_NUM_PER_DEV);
		
		context.write(accInfo, NullWritable.get());
	}
}
