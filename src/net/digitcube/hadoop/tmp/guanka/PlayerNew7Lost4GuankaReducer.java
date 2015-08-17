package net.digitcube.hadoop.tmp.guanka;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class PlayerNew7Lost4GuankaReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String newFlag = "-";
		String NL7Flag = "-";
		int maxGuanKa = 0;
		for(OutFieldsBaseModel val : values){
			String suffix = val.getSuffix();
			String data = val.getOutFields()[0];
			if(PlayerNew7Lost4GuankaMapper.SUFFIX_FLAG_NEW.equals(suffix)){
				newFlag = data; 
			}else if(PlayerNew7Lost4GuankaMapper.SUFFIX_FLAG_NL7.equals(suffix)){
				NL7Flag = data;
			}else if(PlayerNew7Lost4GuankaMapper.SUFFIX_FLAG_GUANKA.equals(suffix)){
				int guanka = StringUtil.convertInt(data, 0);
				if(guanka > maxGuanKa){
					maxGuanKa = guanka;
				}
			}
		}
		
		//如果关卡等于0，说明只是新增玩家流失，玩家新增当天没玩过关卡
		String[] valFields = new String[]{
				maxGuanKa+"",
				newFlag,
				NL7Flag
		};
		redValObj.setOutFields(valFields);
		
		String suffix = "GUANKA_NEW_NL7"; 
		if("-".equals(newFlag)){
			suffix = "GUANKA_OLD_PLAYER";
		}
		
		key.setSuffix(suffix);
		context.write(key, redValObj);
	}
}
