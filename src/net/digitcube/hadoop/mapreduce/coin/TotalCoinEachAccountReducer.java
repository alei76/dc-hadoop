package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 每日虚拟币汇总 Reduce-1
 * 计算每个账号每日的虚拟币留存
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
 * 其依赖的Map 输出：
 * key:[appID, platform, channel, gameServer,accountID]
 * value:[seq,coinnum]
 * 
 * Reduce 输出：
 * [appID, platform, channel, gameServer,accountID,CoinNum] suffix:TOTAL_COIN_EACH_ACCOUNT
 */
@Deprecated
public class TotalCoinEachAccountReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, IntWritable> {

	private IntWritable valObj = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		int maxSeq = 0;
		int finalCoinNum = 0;
		for (OutFieldsBaseModel baseModel : values) {
			String[] valueArr = baseModel.getOutFields();
			int seqno = StringUtil.convertInt(valueArr[0], 0);
			int coin = StringUtil.convertInt(valueArr[1], 0);
			if (seqno > maxSeq) {
				maxSeq = seqno;
				finalCoinNum = coin;
			}
		}
		valObj.set(finalCoinNum);
		key.setSuffix(Constants.SUFFIX_TOTAL_COIN_EACH_ACCOUNT);
		context.write(key, valObj);
	}
}
