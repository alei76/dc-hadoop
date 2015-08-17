package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 每日虚拟币汇总 Map-1
 * Title: CoinLostOrGainMapper.java<br>
 * Description: CoinLostOrGainMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-10-31         <br>
 * @version 1.0
 * <br>
 * 
 * 主要逻辑：
 * 对每个玩家当天上报的金币总量进行去重
 * 得到每个玩家当天最后一次上报的金币总量
 * 
 * 输入：
 * 依赖自定义事件分离 @EventSeparatorMapper 的结果（后缀为 DESelf_Coin_Num 存量虚拟币）
 * 
 * Map 输出：
 * key:[appID, platform, channel, gameServer,accountID]
 * value:[seq,coinnum]
 * 
 * Reduce 输出：
 * [appID, platform, channel, gameServer,accountID, finalCoinNum]
 */
@Deprecated
public class TotalCoinEachAccountMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);

		String[] keyFields = null;
		String[] valueFields = null;

		String appID = paraArr[1];
		String accountID = paraArr[3];
		String platform = paraArr[4];
		String channel = paraArr[5];
		String gameServer = paraArr[9];

		String eventAttr = paraArr[paraArr.length - 1];
		// 取属性 roomId
		String[] attrArr = eventAttr.split(",");
		String seq = "";
		String coinNum = "";
		for (String attr : attrArr) {
			String[] keyValue = attr.split(":");
			String _key = keyValue[0];
			if (_key.equals("total")) {
				coinNum = keyValue[1];
			}
			if (_key.equals("seq")) {
				seq = keyValue[1];
			}
		}
		if (StringUtil.isEmpty(coinNum) || StringUtil.isEmpty(seq)) {
			return;
		}

		keyFields = new String[] { appID, platform, channel, gameServer, accountID };
		valueFields = new String[] { seq, coinNum };
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valueFields);

		context.write(keyObj, valObj);
	}
}
