package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author 
 * 输出：
 * key : appid,platform,channel,gameRegion
 * value : newActDeviceNum(激活设备数量),newOnlineFirNum(新增玩家数量),newADOnlineFirNum(激活设备中新增的玩家),newPayFirNum(付费玩家数量)
 *
 */
public class NewAddStatisticsReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int newActDeviceNum = 0;
		int newOnlineFirNum = 0;
		int newPayFirNum = 0;
		int newADOnlineFirNum = 0;

		for(Text val : values){
			String dataFlag = val.toString();
			if(Constants.DATA_FLAG_DEVICE_ACT.equals(dataFlag)){
				newActDeviceNum++;
			}else if(Constants.DATA_FLAG_FIRST_ONLINE.equals(dataFlag)){
				newOnlineFirNum++;
			}else if(Constants.DATA_FLAG_FIRST_PAY.equals(dataFlag)){
				newPayFirNum++;
			}else if(Constants.DATA_FLAG_AD_FONLINE.equals(dataFlag)){
				newADOnlineFirNum++;
			}
		}
		
		String[] valFields = new String[]{
											"" + newActDeviceNum,
											"" + newOnlineFirNum,
											"" + newADOnlineFirNum,
											"" + newPayFirNum
										 };
		valObj.setOutFields(valFields);
		
		key.setSuffix(Constants.SUFFIX_NEW_ADD_STATISTICS);
		
		context.write(key, valObj);
	}
}

