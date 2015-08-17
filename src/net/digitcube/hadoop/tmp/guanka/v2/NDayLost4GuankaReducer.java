package net.digitcube.hadoop.tmp.guanka.v2;

import java.io.IOException;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class NDayLost4GuankaReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String newFlag = "-";
		String lostFlag = "-";
		int maxGuanKa = 0;
		for(OutFieldsBaseModel val : values){
			String suffix = val.getSuffix();
			if(NDayLost4GuankaMapper.SUFFIX_FLAG_NEW.equals(suffix)){
				newFlag = val.getOutFields()[0];
				lostFlag = val.getOutFields()[1];
			}else if(NDayLost4GuankaMapper.SUFFIX_FLAG_GUANKA.equals(suffix)){
				int guanka = StringUtil.convertInt(val.getOutFields()[0], 0);
				if(guanka > maxGuanKa){
					maxGuanKa = guanka;
				}
			}
		}
		
		//如果关卡等于0，说明只是新增玩家流失，玩家新增当天没玩过关卡
		String[] valFields = new String[]{
				maxGuanKa+"",
				newFlag,
				lostFlag
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
