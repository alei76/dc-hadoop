package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
  * 
 * 主要逻辑：
 * 对每个 app 在 version 、 手机平台 、推广平台等 维度统计 PV 和跳出次数
 * 
 * 
 * 输入：H5PageViewForPlayer
 * appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId, 
 * isNewPlayer, playerTotalPVs, player1ViewPvs, playerPVRecords
 * 
 * map：
 * key = appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, playerType
 * val = playerTotalPVs, player1ViewPvs
 * 
 * reduce：
 * for val : values
 * do
 *     totalPVs += val. playerTotalPVs
 *     oneViewPvs += val.player1ViewPvs
 * done
 *  
 * 输出：
 * 
 * appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, playerType, 
 * totalPVs, oneViewPvs
 * 
 */

public class H5NewPageViewForAppMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String accountId = arr[i++];
		String platform = arr[i++];
		String H5_PromotionAPP = arr[i++];
		String H5_DOMAIN = arr[i++];
		String H5_REF = arr[i++];		
		String playerType = arr[i++];
		String playerTotalPVs = arr[i++];
		String player1ViewPvs = arr[i++];
		String playerPVRecords = arr[i++];
		
		//活跃玩家
		String[] keyFields = new String[] { 
				appId,
				platform,
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				playerType
		};
		String[] valFields = new String[]{
				playerTotalPVs,
				player1ViewPvs
		};
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);	
	}
}
