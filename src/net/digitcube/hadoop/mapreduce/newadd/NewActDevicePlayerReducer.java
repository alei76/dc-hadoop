package net.digitcube.hadoop.mapreduce.newadd;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author 
 * 输出：
 * key : appid,platform,channel,gameRegion
 * value : newActDeviceNum(激活设备数量),newADOnlineFirNum(激活设备中新增的玩家)
 *
 */
public class NewActDevicePlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		
		if(Constants.SUFFIX_ACCOUNT_NUM_PER_DEV.equals(key.getSuffix())){
			//单台设备帐号数统计
			int totalAccountNum = 0;
			for(OutFieldsBaseModel val : values){
				totalAccountNum++;
			}
			
			String[] valFields = new String[]{totalAccountNum+""};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
			
		}else if(Constants.SUFFIX_NEW_ADD_DEV_PLAYER.equals(key.getSuffix())){
			//新增激活设备数
			int newActiveDeviceNum = 0; 
			//激活设备中的新增玩家数
			int newAddPlayerFromDevice = 0; 
			//新增玩家数
			int newAddPlayerNum = 0;
			//新增付费玩家数
			int newPayPlayerNum = 0;
			
			for(OutFieldsBaseModel val : values){
				String dataFlag = val.getOutFields()[0];
				int num = StringUtil.convertInt(val.getOutFields()[1], 0);
				if(Constants.DATA_FLAG_NEWACT_DEVICE.equals(dataFlag)){
					newActiveDeviceNum += num;
					
				}else if(Constants.DATA_FLAG_ACTDEV_PLAYER.equals(dataFlag)){
					newAddPlayerFromDevice += num;
					
				}else if(Constants.DATA_FLAG_NEWADD_PLAYER.equals(dataFlag)){
					newAddPlayerNum += num;
					
				}else if(Constants.DATA_FLAG_NEWPAY_PLAYER.equals(dataFlag)){
					newPayPlayerNum += num;
				} 
			}
			
			String[] valFields = new String[]{
					"" + newActiveDeviceNum,
					"" + newAddPlayerNum,
					"" + newAddPlayerFromDevice,
					"" + newPayPlayerNum
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}
	}
}

