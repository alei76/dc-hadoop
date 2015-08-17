package net.digitcube.hadoop.mapreduce.cards;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * see @ValidOnlinePalyerMapper
 */
public class ValidPlayerReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	private Set<String> onlineDaysSet = new HashSet<String>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		onlineDaysSet.clear();
		
		// 是否新增玩家(正常逻辑下，新增玩家只会出现一次)
		/*boolean isNewAddPlayer = false;
		String[] ramdonOne = null;
		int playCardRounds = 0;
		for(OutFieldsBaseModel val : values){
			
			String dataFlag = val.getOutFields()[0];
			if(ValidPlayerMapper.DATA_FLAG_NEW_ADD.equals(dataFlag)){
				isNewAddPlayer = true;
				
			}else{ 
				// 不是新增玩家记录那就是玩牌据说记录
				String onlineDate = val.getOutFields()[1];
				onlineDaysSet.add(onlineDate);
				
				String playRounds = val.getOutFields()[2];
				int rounds = StringUtil.convertInt(playRounds, 0);
				playCardRounds += rounds; //玩牌局数
				
				if(null == ramdonOne){
					ramdonOne = val.getOutFields();
				}
			}
		}*/
		
		String[] ramdonOne = null;
		int newPlayCardRounds = 0;
		int playCardRounds = 0;
		for(OutFieldsBaseModel val : values){
			
			//记录打牌的日期
			String playDate = val.getOutFields()[0];
			onlineDaysSet.add(playDate);
			
			//以新增玩家身份玩牌局数（只有新增当天 newPlayRounds 才会大于 0 ）
			String newPlayRounds = val.getOutFields()[1];
			newPlayCardRounds += StringUtil.convertInt(newPlayRounds, 0);
			
			//以活约玩家身份玩牌局数（总是大于 0 ）
			String actPlayRounds = val.getOutFields()[2];
			playCardRounds += StringUtil.convertInt(actPlayRounds, 0);
			
			if(null == ramdonOne){
				ramdonOne = val.getOutFields();
			}
		}
		
		if(null == ramdonOne){
			return;
		}
		String[] valFields = new String[]{
						newPlayCardRounds > 0 ? "1" : "0",	//是否新增玩家(不管是否新增都视为活跃玩家)
						ramdonOne[3], //channel
						ramdonOne[4], //accountType
						ramdonOne[5], //area
						ramdonOne[6], //age
						ramdonOne[7] //gender
		};
		
		redValObj.setOutFields(valFields);
		
		
		key.setSuffix(Constants.SUFFIX_VALID_PLAYER);
		String validDateFlag = key.getOutFields()[key.getOutFields().length - 1];
		
		// 30 日有效玩家条件：在线天数至少 5 日/玩牌局数至少 15 局
		// 7 日有效玩家条件：在线天数至少 2 日/玩牌局数至少 6 局
		// 1 日有效玩家条件：玩牌局数至少 3 局
		if(Constants.PLAYER_VALID_30.equals(validDateFlag) && onlineDaysSet.size() >= 5 && playCardRounds >= 15 ){
			context.write(key, redValObj);
		}else if(Constants.PLAYER_VALID_7.equals(validDateFlag) && onlineDaysSet.size() >= 2 && playCardRounds >= 6 ){
			context.write(key, redValObj);
		}else if(Constants.PLAYER_VALID_1.equals(validDateFlag) && playCardRounds >= 3 ){
			context.write(key, redValObj);
		}
	}
}

