package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class CHGameCenterReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		if(Constants.SUFFIX_CHANNEL_GAME_CENTER.equals(key.getSuffix())){
			int totalPlayerNum = 0;
			int newPlayerNum = 0;
			int downloadPlayerNum = 0;
			int downloadTimes = 0;
			for(OutFieldsBaseModel val : values){
				String suffix = val.getSuffix();
				int count = StringUtil.convertInt(val.getOutFields()[0], 0);
				if(CHGameCenterMapper.DATA_FLAG_ALL_USER.equals(suffix)){
					totalPlayerNum+=count;
				}else if(CHGameCenterMapper.DATA_FLAG_NEW_USER.equals(suffix)){
					newPlayerNum+=count;
				}else if(CHGameCenterMapper.DATA_FLAG_DLD_USERS.equals(suffix)){
					downloadPlayerNum+=count;
				}else if(CHGameCenterMapper.DATA_FLAG_DLD_TIMES.equals(suffix)){
					downloadTimes+=count;
				}
			}
			
			String[] valFields = new String[]{
					totalPlayerNum+"",
					newPlayerNum+"",
					downloadPlayerNum+"",
					downloadTimes+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
			
		}else if(Constants.SUFFIX_CHANNEL_DLD_TIMES_LAYOUT.equals(key.getSuffix())){
			int total = 0;
			for(OutFieldsBaseModel val : values){
				total+=StringUtil.convertInt(val.getOutFields()[0], 0);
			}
			String[] valFields = new String[]{
					total+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}else if(Constants.SUFFIX_CHANNEL_DLD_NUMS_LAYOUT.equals(key.getSuffix())){
			int total = 0;
			for(OutFieldsBaseModel val : values){
				total+=StringUtil.convertInt(val.getOutFields()[0], 0);
			}
			String[] valFields = new String[]{
					total+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}else if(Constants.SUFFIX_CHANNEL_PLAYER_DLD_NUM.equals(key.getSuffix())){
			int total = 0;
			for(OutFieldsBaseModel val : values){
				total++;
			}
			String[] valFields = new String[]{
					total+""
			};
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}
	}
}
