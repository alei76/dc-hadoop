package net.digitcube.hadoop.mapreduce.layout;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 逻辑
 * 输入日志：玩家首付信息
 * 依赖 @UserInfoRollingDayMapper 的结果（后缀为 FIRST_PAY_DAY）
 * 在 UserInfoRollingDay 滚存 MR 中，将判断玩家付费是否是首付
 * 如果是的话，则输出该玩家首付时的信息，包括：
 * 首付时的在线天数、时长、级别、首付金额
 * 本 MR 则利用上述数据进行首付分布统计，得到最终结果
 * 
 * 输入：
 * appId, platform, channel, gameServer, dimenType(维度类型，如首付金额、游戏天数), dimenRange(该维度范围，如 10~20天)
 * 输出
 * key: appId, platform, channel, gameServer, dimenType(维度类型，如首付金额、游戏天数), dimenRange(该维度范围，如 10~20天)
 * value: 1
 */

public class LayoutOnFirstPayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, FloatWritable> {
	
	private final static FloatWritable val = new FloatWritable();
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = array[i++];
		String platform = array[i++];
		String channel = array[i++];
		String gameServer = array[i++];
		int firstPayOnlineDay = StringUtil.convertInt(array[i++], 0);
		int firstPayOnlineTime = StringUtil.convertInt(array[i++], 0);
		// 20141216 : 玩家首付等级改为具体值，不用区间
		//int firstPayLevel = StringUtil.convertInt(array[i++], 0);
		String firstPayLevel = array[i++];
		String firstPayCurrency = array[i++];
		int firstPayCurrencyInt = StringUtil.convertInt(firstPayCurrency, 0);
		float firstPayCurrencyFloat = StringUtil.convertFloat(firstPayCurrency, 0);
		
		// 获取首付时的在线天数、时长、级别、首付金额范围，用于维度统计
		int onlineDayRange = EnumConstants.getFirstPayGameDaysRange(firstPayOnlineDay);
		int onlineTimeRange = EnumConstants.getPayTimeRange(firstPayOnlineTime);
		//int levelRange = EnumConstants.getFirstPayLevelRange(firstPayLevel);
		int currencyRange = EnumConstants.getFirstPayCurRange(firstPayCurrencyInt);
		if("".equals(firstPayLevel) || "0".equals(firstPayLevel)){
			firstPayLevel = "1";
		}
				
		val.set(1);//计数
		mapKeyObj.setSuffix(Constants.SUFFIX_LAYOUT_ON_FIRST_PAY);//输出标识
		
		// 首付时玩家游戏天数
		String[] gameDaysArr = new String[] { 
				appId,
				platform,
				channel,
				gameServer,
				Constants.DIMENSION_PLAYER_DAYS, 
				"" + onlineDayRange };
		mapKeyObj.setOutFields(gameDaysArr);
		context.write(mapKeyObj, val);

		// 首付时玩家游戏时长
		String[] gameTimeArr = new String[] { 
				appId,
				platform,
				channel,
				gameServer,
				Constants.DIMENSION_PLAYER_ONLINETIME, 
				"" + onlineTimeRange };
		mapKeyObj.setOutFields(gameTimeArr);
		context.write(mapKeyObj, val);

		// 首付时玩家游戏等级
		String[] gameLevelArr = new String[] { 
				appId,
				platform,
				channel,
				gameServer,
				Constants.DIMENSION_PLAYER_LEVEL, 
				firstPayLevel };
		mapKeyObj.setOutFields(gameLevelArr);
		context.write(mapKeyObj, val);

		// 首付金额 currency 区间分布
		String[] gameCurrencyArr = new String[] {
				appId,
				platform,
				channel,
				gameServer,
				Constants.DIMENSION_FIRST_PAY_GAMECURR, 
				"" + currencyRange };
		mapKeyObj.setOutFields(gameCurrencyArr);
		context.write(mapKeyObj, val);
		
		// 首次付费金额统计
		val.set(firstPayCurrencyFloat);//首付金额
		mapKeyObj.setSuffix(Constants.SUFFIX_FIRST_PAY_CURRENCY);//输出标识
		String[] firstPayCurrencyArr = new String[] {
				appId,
				platform,
				channel,
				gameServer,
				Constants.PLAYER_TYPE_NEWADD, //与向前约定玩家类型用  1
		};
		mapKeyObj.setOutFields(firstPayCurrencyArr);
		context.write(mapKeyObj, val);
	}
	
}
