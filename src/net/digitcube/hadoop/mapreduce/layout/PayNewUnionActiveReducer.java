package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * 输入：
 * key：appId, accountId, platform
 * value：channel, gameServer, loginRecords
 * 
 * 输出：
 * key：appId, playType, platform
 * value：channel, gameServer, loginRecords
 */

public class PayNewUnionActiveReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {

		OutFieldsBaseModel newAddPlayer = null;
		OutFieldsBaseModel onlinePlayer = null;
		OutFieldsBaseModel payPlayer = null;
		
		for(OutFieldsBaseModel val : values){
			if(Constants.PLAYER_TYPE_NEWADD.equals(val.getSuffix())){
				newAddPlayer = new OutFieldsBaseModel(val.getOutFields());
				
			}else if(Constants.PLAYER_TYPE_ONLINE.equals(val.getSuffix())){
				onlinePlayer = new OutFieldsBaseModel(val.getOutFields());
				
			}else if(Constants.PLAYER_TYPE_PAYMENT.equals(val.getSuffix())){
				payPlayer = new OutFieldsBaseModel(val.getOutFields());
			}
		}
		
		// 设置输出后缀
		key.setSuffix(Constants.SUFFIX_PAY_NEW_UNION_ACT);
		
		// 输出活跃玩家的在线信息
		if(null != onlinePlayer){
			key.getOutFields()[1] = Constants.PLAYER_TYPE_ONLINE;
			context.write(key, onlinePlayer);
		}
		
		// 输出新增玩家的在线信息
		if(null != newAddPlayer && null != onlinePlayer){ 
			// 新增用户和活跃用户关联成功
			// 活跃用户的登录信息即是新增用户信息
			// 把活跃用户的登录信息输出即可，但用户类型需设置为新增
			key.getOutFields()[1] = Constants.PLAYER_TYPE_NEWADD;
			context.write(key, onlinePlayer);
		}
		
		// 输出付费玩家的在线信息
		if(null != payPlayer && null != onlinePlayer){ 
			// 付费用户和活跃用户关联成功
			// 活跃用户的登录信息即是付费用户信息
			// 把活跃用户的登录信息输出即可，但用户类型需设置为付费
			key.getOutFields()[1] = Constants.PLAYER_TYPE_PAYMENT;
			context.write(key, onlinePlayer);
		}
	}

}
