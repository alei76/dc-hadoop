package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 每日虚拟币汇总 Reduce-2
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
 * 其依赖的Map 输出：
 * [appID, platform, channel, gameServer,coinNum] Constants.SUFFIX_TOTAL_COIN
 */
@Deprecated
public class TotalCoinReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, LongWritable> {
	private LongWritable valObj = new LongWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		long sum = 0;
		for (IntWritable baseModel : values) {
			sum += baseModel.get();
		}
		valObj.set(sum);
		key.setSuffix(Constants.SUFFIX_TOTAL_COIN);
		context.write(key, valObj);
	}
}
