package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class PlayerInfoForExtReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String playerType7 = Constants.DATA_FLAG_PLAYER_ONLINE; // 只是活跃
		String playerType30 = Constants.DATA_FLAG_PLAYER_ONLINE; // 只是活跃		
		
		String[] valFields = null;
		String[] valFieldsNewPay = null;
		String firstLoginDate = null;
		String totalPayCur = null;
		String detailInfo = null;
		
		for(OutFieldsBaseModel val : values){			
			if("EXTROLL".equals(val.getSuffix())){				
				valFields = val.getOutFields();
				detailInfo = valFields[1];					
			}else if("PLAYERNEWPAY".equals(val.getSuffix())){				
				valFieldsNewPay = val.getOutFields();
				firstLoginDate = valFieldsNewPay[0];
				totalPayCur = valFieldsNewPay[1];
				// 7天玩家类型
				boolean isNewPlayer = DateUtil.getDateBeforeDays(7,statTime) <= StringUtil.convertInt(firstLoginDate, 0) 
						&& statTime >= StringUtil.convertInt(firstLoginDate, 0);
				boolean isPayToday = StringUtil.convertInt(totalPayCur, 0) > 0;
				if (isNewPlayer && isPayToday) { // 集新增、活跃、付费于一身
					playerType7 = Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY;
				} else if (isNewPlayer) { // 只是新增、活跃
					playerType7 = Constants.DATA_FLAG_PLAYER_NEW_ONLINE;
				} else if (isPayToday) { // 只是活跃、付费
					playerType7 = Constants.DATA_FLAG_PLAYER_ONLINE_PAY;
				}
				// 30天玩家类型
				isNewPlayer = DateUtil.getDateBeforeDays(30,statTime) <= StringUtil.convertInt(firstLoginDate, 0) 
						&& statTime >= StringUtil.convertInt(firstLoginDate, 0);
				if (isNewPlayer && isPayToday) { // 集新增、活跃、付费于一身
					playerType30 = Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY;
				} else if (isNewPlayer) { // 只是新增、活跃
					playerType30 = Constants.DATA_FLAG_PLAYER_NEW_ONLINE;
				} else if (isPayToday) { // 只是活跃、付费
					playerType30 = Constants.DATA_FLAG_PLAYER_ONLINE_PAY;
				}				
			}
		}
		if(null == valFields || StringUtil.isEmpty(playerType7)
				|| StringUtil.isEmpty(playerType30)){
			return;
		}
		
		String[] outFields = new String[]{
				valFields[0], // channel
				playerType7,
				playerType30,
				detailInfo
		};
		valObj.setOutFields(outFields);
		key.setSuffix(Constants.SUFFIX_PLAYER_INFO_FOR_EXT);				
		context.write(key, valObj);
		
		
		
	}
	
}
